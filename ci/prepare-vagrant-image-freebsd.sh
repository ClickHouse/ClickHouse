#!/usr/bin/env bash
set -e -x

source default-config

$SUDO apt-get -y install vagrant virtualbox

pushd "vagrant-freebsd"
vagrant up
vagrant ssh-config > vagrant-ssh
ssh -F vagrant-ssh default 'uname -a'
scp -F vagrant-ssh -r ../../ci default:~
popd
