#!/bin/bash

sudo apt-get install docker.io
sudo docker run --network host -it --volume=$(pwd):/workspace ubuntu:18.04

# https://greenplum.org/install-greenplum-oss-on-ubuntu/
apt-get update
apt install software-properties-common
add-apt-repository ppa:greenplum/db
apt-get update
apt install greenplum-db-6

echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen
dpkg-reconfigure --frontend=noninteractive locales

useradd ubuntu
mkdir /home/ubuntu
chown ubuntu /home/ubuntu
su ubuntu
bash
cd ~

export GPHOME=/opt/greenplum-db-*
cp $GPHOME/docs/cli_help/gpconfigs/gpinitsystem_singlenode .

source /opt/greenplum-db-*/greenplum_path.sh
echo $(hostname) > ./hostlist_singlenode



cd workspace
