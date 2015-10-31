#!/bin/sh

g++ -std=c++11 -Wall -Werror -g -static-libstdc++ -static-libgcc -shared -fPIC -Wl,-rpath=/usr/local/lib:/usr/local/lib64 -lPocoFoundation -lPocoNet -o odbc.so *.cpp
