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
}' | dd of=/etc/docker/daemon.json 2>/dev/null

if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
    # move the processes from the root group to the /init group,
    # otherwise writing subtree_control fails with EBUSY.
    # An error during moving non-existent process (i.e., "cat") is ignored.
    mkdir -p /sys/fs/cgroup/init
    xargs -rn1 < /sys/fs/cgroup/cgroup.procs > /sys/fs/cgroup/init/cgroup.procs || :
    # enable controllers
    sed -e 's/ / +/g' -e 's/^/+/' < /sys/fs/cgroup/cgroup.controllers \
        > /sys/fs/cgroup/cgroup.subtree_control
fi

# Binding to an IP address without --tlsverify is deprecated. Startup is intentionally being slowed
# unless --tls=false or --tlsverify=false is set
#
# In case of test hung it is convenient to use pytest --pdb to debug it,
# and on hung you can simply press Ctrl-C and it will spawn a python pdb,
# but on SIGINT dockerd will exit, so we spawn new session to ignore SIGINT by
# docker.
# Note, that if you will run it via runner, it will send SIGINT to docker anyway.
setsid dockerd --host=unix:///var/run/docker.sock --tls=false --host=tcp://0.0.0.0:2375 --default-address-pool base=172.17.0.0/12,size=24 &>/ClickHouse/tests/integration/dockerd.log &

set +e
reties=0
while true; do
    docker info &>/dev/null && break
    reties=$((reties+1))
    if [[ $reties -ge 100 ]]; then # 10 sec max
        echo "Can't start docker daemon, timeout exceeded." >&2
        exit 1;
    fi
    sleep 0.1
done
set -e

# cleanup for retry run if volume is not recreated
{
    docker ps --all --quiet | xargs --no-run-if-empty docker kill || true
    docker ps --all --quiet | xargs --no-run-if-empty docker rm || true
}

java_path="$(update-alternatives --config java | sed -n 's/.*(providing \/usr\/bin\/java): //p')"
export JAVA_PATH=$java_path
export SPARK_HOME="/spark-3.5.5-bin-hadoop3"
export PATH=$SPARK_HOME/bin:$PATH
export JAVA_TOOL_OPTIONS="-Djdk.attach.allowAttachSelf=true"

echo "Start tests"
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=/clickhouse
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=/clickhouse
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=/clickhouse-config

export DOCKER_BASE_TAG=${DOCKER_BASE_TAG:=latest}
export DOCKER_DOTNET_CLIENT_TAG=${DOCKER_DOTNET_CLIENT_TAG:=latest}
export DOCKER_HELPER_TAG=${DOCKER_HELPER_TAG:=latest}
export DOCKER_KERBERIZED_HADOOP_TAG=${DOCKER_KERBERIZED_HADOOP_TAG:=latest}
export DOCKER_KERBEROS_KDC_TAG=${DOCKER_KERBEROS_KDC_TAG:=latest}
export DOCKER_MYSQL_GOLANG_CLIENT_TAG=${DOCKER_MYSQL_GOLANG_CLIENT_TAG:=latest}
export DOCKER_MYSQL_JAVA_CLIENT_TAG=${DOCKER_MYSQL_JAVA_CLIENT_TAG:=latest}
export DOCKER_MYSQL_JS_CLIENT_TAG=${DOCKER_MYSQL_JS_CLIENT_TAG:=latest}
export DOCKER_MYSQL_PHP_CLIENT_TAG=${DOCKER_MYSQL_PHP_CLIENT_TAG:=latest}
export DOCKER_NGINX_DAV_TAG=${DOCKER_NGINX_DAV_TAG:=latest}
export DOCKER_POSTGRESQL_JAVA_CLIENT_TAG=${DOCKER_POSTGRESQL_JAVA_CLIENT_TAG:=latest}

cd /ClickHouse/tests/integration
exec "$@"
