#!/usr/bin/env bash
set -e

source default-config

sudo apt-get -y install vagrant virtualbox

pushd "vagrant-freebsd"
vagrant up
popd
