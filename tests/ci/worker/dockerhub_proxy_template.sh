#!/usr/bin/env bash
set -xeuo pipefail

mkdir /home/ubuntu/registrystorage

sed -i 's/preserve_hostname: false/preserve_hostname: true/g' /etc/cloud/cloud.cfg

docker run -d --network=host -p 5000:5000 -v /home/ubuntu/registrystorage:/var/lib/registry -e REGISTRY_HTTP_ADDR=0.0.0.0:5000 -e REGISTRY_STORAGE_DELETE_ENABLED=true -e REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io --restart=always --name registry registry:2
