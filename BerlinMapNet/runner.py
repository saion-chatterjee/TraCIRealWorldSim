#!/usr/bin/env python
"""
@file    runner.py
@author  Saion Chatterjee
@date    2017

"""
from __future__ import absolute_import
from __future__ import print_function
from kafka import *

import os
import sys
import optparse
import subprocess
import random

# we need to import python modules from the $SUMO_HOME/tools directory
try:
    sys.path.append(os.path.join(os.path.dirname(
        __file__), '..', '..', '..', '..', "tools"))  # tutorial in tests
    sys.path.append(os.path.join(os.environ.get("SUMO_HOME", os.path.join(
        os.path.dirname(__file__), "..", "..", "..")), "tools"))  # tutorial in docs
    from sumolib import checkBinary
except ImportError:
    sys.exit(
        "please declare environment variable 'SUMO_HOME' as the root directory of your sumo installation (it should contain folders 'bin', 'tools' and 'docs')")

import traci
import sumolib
#from sumolib.net import Net
net = sumolib.net.readNet('osm.net.xml')
import traci.constants as tc

def run(noChangeOfTarget):
    """execute the TraCI control loop"""
    step = 0
#    myKafka = KafkaClient("vm-10-155-208-120.cloud.mwn.de:6667")

    #lat = 52.525415
    #lon = 13.398882
    
    lat = 52.55192
    lon = 13.40123

    blinkFlag = False
    blinkVar = 0
    i=10
    j=10
    
    XConvReco, YConvReco = net.convertLonLat2XY(13.40514971317243, 52.51755499453762, rawUTM=False)
    #print("polyCentre: ", [lonConvReco, latConvReco])
    list1 = [(XConvReco-i,YConvReco+j),(XConvReco+i,YConvReco+j),(XConvReco+i,YConvReco-j),(XConvReco-i,YConvReco-j),(XConvReco-i,YConvReco+j)]
    #start dummy add
    traci.polygon.add('1', list1, (255,0,0,0), False, '1',50)
    traci.polygon.add('2', list1, (0,255,0,0), False, '1',50)

    producer, consumer = setUpKafka()

    #Test
#    lonTest=1295.059694378113
#    latTest=982.6744777355343

    #Restaurant basic setup
    traci.poi.add('1', 100, 120, (255,0,0,0), '1',0)
    traci.poi.add('2', 300, 220, (255,0,0,0), '2',0)
    traci.poi.add('3', 520, 220, (255,0,0,0), '3',0)
    traci.poi.add('4', 580, 410, (255,0,0,0), '2',0)
    traci.poi.add('5', 590, 230, (255,0,0,0), '4',0)
    traci.poi.add('6', 610, 320, (255,0,0,0), '1',0)


    #runKafkaProd(producer,lat)    
    
    #Subscribe to cars
    traci.simulation.subscribe((tc.VAR_ARRIVED_VEHICLES_IDS,))

    while traci.simulation.getMinExpectedNumber() > 0:
        traci.simulationStep()
        step += 1
        
        vehIDList = traci.simulation.getSubscriptionResults()[122]
        
        if('veh0' in vehIDList):
            traci.vehicle.rerouteEffort('veh0')    
        
        #get vehicle position
        tempPos = traci.vehicle.getPosition('veh0')

        #print("PositionGeo: ", traci.simulation.convertGeo(tempPos[0],tempPos[1], False))
        #print("PositionGeo: ", net.convertXY2LonLat(tempPos[0],tempPos[1], False))
                    
        lonConv, latConv = net.convertXY2LonLat(tempPos[0],tempPos[1], False)
        runKafkaProd(producer,latConv,step)    

        #print("PositionGeo: ", net.convertLonLat2XY(lonConv, latConv, False))

        #producer.send_messages('MyKafkaTest', bytes([step,lat]))
        #lat = lat + 0.0005
        
        #convert lat to sumo coordinates
        #traci.simulation.convert2D(tempPos[0],tempPos[1]

        #update polygon
#        if(step%5 == 0):
        XCar,YCar = net.convertLonLat2XY(lonConv, latConv, False)
        print("NewBox: ", XCar, YCar)

        traci.polygon.remove('1',50)               
#        list1 = [(600+i,100+j),(600+i,200+j),(700+i,200+j),(700+i,100+j),(600+i,100+j)]
        list1 = [(XCar-i,YCar+j),(XCar+i,YCar+j),(XCar+i,YCar-j),(XCar-i,YCar-j),(XCar-i,YCar+j)]
        traci.polygon.add('1', list1, (255,0,0,0), False, '1',50)

        #update reco box
        if(step%10 == 0):
            for msg in consumer.get_messages(count=1, block=True, timeout=2):
                print("Consumed: ", msg)
        latReco = 52.525415
        lonReco = 13.40123
        XReco,YReco = net.convertLonLat2XY(lonReco, latReco, False)
        traci.polygon.remove('2',50)
        print("Reco: ", XReco, YReco)
        list2 = [(XReco-i,YReco+j),(XReco+i,YReco+j),(XReco+i,YReco-j),(XReco-i,YReco-j),(XReco-i,YReco+j)]
        traci.polygon.add('2', list2, (0,255,0,0), False, '1',50)

        
        if (blinkFlag == True):
            # Highlight reco restau
            #traci.polygon.add('1', list((570,400)), (0,0,255,0), False, '1', 0)
            blinkVar = blinkVar+1
            if(blinkVar%2 == 0):
                traci.poi.setColor('4', (0,255,0,0))
            else:
                traci.poi.setColor('4', (255,0,0,0))

        if (step == 30) and not noChangeOfTarget:
            # Receive Restau Id recommendation from kafka response topic
            blinkFlag = True
            
        if (step == 90) and not noChangeOfTarget:
            # Restau reco
            blinkFlag = False

        if (step == 40) and not noChangeOfTarget:
            print("Now changing direction and turning up at the first intersection!")
            # traci.vehicle.changeTarget("veh0", "2232")
	            
    traci.close()
    sys.stdout.flush()

def setUpKafka():
    # producer = KafkaProducer(bootstrap_servers='vm-10-155-208-120.cloud.mwn.de:6667')
    print("setup kafka")
    ProdClient = KafkaClient("vm-10-155-208-120.cloud.mwn.de:6667")
    ConsClient = KafkaClient("vm-10-155-208-120.cloud.mwn.de:6667") #Creating two different client instances for threading

    producer = SimpleProducer(ProdClient)
    consumer = SimpleConsumer(ConsClient,"group","MyTest")
    return producer, consumer

def runKafkaProd(producer,lat,step):
    message = str(lat) + "," + str(step)
    producer.send_messages('MyKafkaTest', bytes(message))


def get_options():
    optParser = optparse.OptionParser()
    optParser.add_option("--nogui", action="store_true",
                         default=False, help="run the commandline version of sumo")
    optParser.add_option("--noChangeOfTarget", action="store_true",
                     default=False, help="whether to change target (destination) at tick 40")
    options, args = optParser.parse_args()
    return options

if __name__ == "__main__":
    options = get_options()

    # this script has been called from the command line. It will start sumo as a
    # server, then connect and run
    if options.nogui:
        sumoBinary = checkBinary('sumo')
    else:
        sumoBinary = checkBinary('sumo-gui')

    # this is the normal way of using traci. sumo is started as a
    # subprocess and then the python script connects and runs
    traci.start([sumoBinary, "-c", "osm.sumocfg"])
    run(options.noChangeOfTarget)
