#!/usr/bin/env bash

CONFIG=config

cd "$(dirname $0)"/../..

./get-sources.sh
./prepare-toolchain.sh
./install-libraries.sh
./build-normal.sh
