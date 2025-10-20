#!/bin/bash
set -e

mkdir -p /etc/docker/
echo '{
    "ipv6": true,
    "fixed-cidr-v6": "fd00::/8",
    "ip-forward": true,
    "log-level": "debug",
    "storage-driver": "overlay2",
    "insecure-registries" : ["dockerhub-proxy.dockerhub-proxy-zone:5000"],
    "registry-mirrors" : ["http://dockerhub-proxy.dockerhub-proxy-zone:5000"]
}' | dd of=/etc/docker/daemon.json

# if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
#     # move the processes from the root group to the /init group,
#     # otherwise writing subtree_control fails with EBUSY.
#     # An error during moving non-existent process (i.e., "cat") is ignored.
#     mkdir -p /sys/fs/cgroup/init
#     xargs -rn1 < /sys/fs/cgroup/cgroup.procs > /sys/fs/cgroup/init/cgroup.procs || :
#     # enable controllers
#     sed -e 's/ / +/g' -e 's/^/+/' < /sys/fs/cgroup/cgroup.controllers \
#         > /sys/fs/cgroup/cgroup.subtree_control
# fi

# Binding to an IP address without --tlsverify is deprecated. Startup is intentionally being slowed
# unless --tls=false or --tlsverify=false is set
#
# In case of test hung it is convenient to use pytest --pdb to debug it,
# and on hung you can simply press Ctrl-C and it will spawn a python pdb,
# but on SIGINT dockerd will exit, so we spawn new session to ignore SIGINT by
# docker.
# Note, that if you will run it via runner, it will send SIGINT to docker anyway.
setsid dockerd --host=unix:///var/run/docker.sock --tls=false --host=tcp://0.0.0.0:2375 --default-address-pool base=172.17.0.0/12,size=24
