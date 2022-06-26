#!/bin/bash

sudo apt-get update
sudo apt-get install docker.io
sudo docker run -it --rm --volume=$(pwd):/workspace ubuntu:18.04

cd workspace
apt update
wget --continue 'https://github.com/greenplum-db/gpdb/releases/download/6.21.0/greenplum-db-6.21.0-ubuntu18.04-amd64.deb'

apt install ./greenplum-db-6.21.0-ubuntu18.04-amd64.deb
useradd gpadmin
chown -R gpadmin:gpadmin /usr/local/greenplum*
chgrp -R gpadmin /usr/local/greenplum*
