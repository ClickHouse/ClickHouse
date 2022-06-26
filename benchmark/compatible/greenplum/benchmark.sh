#!/bin/bash

sudo apt-get install -y docker.io
sudo docker run --name greenplum --volume=$(pwd):/workspace gptext/gpdb6
