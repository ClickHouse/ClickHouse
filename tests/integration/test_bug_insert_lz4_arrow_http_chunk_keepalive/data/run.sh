#!/bin/bash

set -eux

echo "Ok"

# tar xfv click-bench-main.tar.gz
cd click-bench-main
./gradlew runClickhouseBench
