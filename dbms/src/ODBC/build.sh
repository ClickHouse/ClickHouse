#!/bin/sh

g++ -std=gnu++1y -Wall -g -shared -fPIC -lPocoFoundation -lPocoNet -o odbc.so *.cpp
