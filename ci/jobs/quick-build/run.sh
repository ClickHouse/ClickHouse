#!/usr/bin/env bash

CONFIG=config

cd ../..

./get-sources.sh
./prepare-toolchain.sh
./install-libraries.sh
./build-normal.sh
