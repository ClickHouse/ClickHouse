#!/usr/bin/env bash
set -xeuo pipefail

bash /usr/local/share/scripts/init-network.sh

# tune sysctl for network performance
cat > /etc/sysctl.d/10-network-memory.conf << EOF
net.core.netdev_max_backlog=2000
net.core.rmem_max=1048576
net.core.wmem_max=1048576
net.ipv4.tcp_max_syn_backlog=1024
net.ipv4.tcp_rmem=4096 131072  16777216
net.ipv4.tcp_wmem=4096 87380   16777216
net.ipv4.tcp_mem=4096  131072  16777216
EOF

sysctl -p /etc/sysctl.d/10-network-memory.conf

mkdir /home/ubuntu/registrystorage

sed -i 's/preserve_hostname: false/preserve_hostname: true/g' /etc/cloud/cloud.cfg

REGISTRY_PROXY_USERNAME=robotclickhouse
REGISTRY_PROXY_PASSWORD=$(aws ssm get-parameter --name dockerhub_robot_password --with-decryption | jq '.Parameter.Value' -r)

docker run -d --network=host -p 5000:5000 -v /home/ubuntu/registrystorage:/var/lib/registry \
  -e REGISTRY_STORAGE_CACHE='' \
  -e REGISTRY_HTTP_ADDR=0.0.0.0:5000 \
  -e REGISTRY_STORAGE_DELETE_ENABLED=true \
  -e REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io \
  -e REGISTRY_PROXY_PASSWORD="$REGISTRY_PROXY_PASSWORD" \
  -e REGISTRY_PROXY_USERNAME="$REGISTRY_PROXY_USERNAME" \
  --restart=always --name registry registry:2
