#!/usr/bin/env bash
set -e -x

source default-config

sudo apt-get install -y zookeeper
$SUDO /usr/share/zookeeper/bin/zkServer.sh start
