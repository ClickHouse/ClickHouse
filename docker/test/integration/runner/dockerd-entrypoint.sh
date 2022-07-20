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

dockerd --host=unix:///var/run/docker.sock --host=tcp://0.0.0.0:2375 --default-address-pool base=172.17.0.0/12,size=24 &>/ClickHouse/tests/integration/dockerd.log &

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
# shellcheck disable=SC2046
{
    docker ps -aq | xargs -r docker kill || true
    docker ps -aq | xargs -r docker rm || true
}

echo "Start tests"
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=/clickhouse
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=/clickhouse
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=/clickhouse-config
export CLICKHOUSE_ODBC_BRIDGE_BINARY_PATH=/clickhouse-odbc-bridge
export CLICKHOUSE_LIBRARY_BRIDGE_BINARY_PATH=/clickhouse-library-bridge

export DOCKER_MYSQL_GOLANG_CLIENT_TAG=${DOCKER_MYSQL_GOLANG_CLIENT_TAG:=latest}
export DOCKER_DOTNET_CLIENT_TAG=${DOCKER_DOTNET_CLIENT_TAG:=latest}
export DOCKER_MYSQL_JAVA_CLIENT_TAG=${DOCKER_MYSQL_JAVA_CLIENT_TAG:=latest}
export DOCKER_MYSQL_JS_CLIENT_TAG=${DOCKER_MYSQL_JS_CLIENT_TAG:=latest}
export DOCKER_MYSQL_PHP_CLIENT_TAG=${DOCKER_MYSQL_PHP_CLIENT_TAG:=latest}
export DOCKER_POSTGRESQL_JAVA_CLIENT_TAG=${DOCKER_POSTGRESQL_JAVA_CLIENT_TAG:=latest}
export DOCKER_KERBEROS_KDC_TAG=${DOCKER_KERBEROS_KDC_TAG:=latest}
export DOCKER_KERBERIZED_HADOOP_TAG=${DOCKER_KERBERIZED_HADOOP_TAG:=latest}

cd /ClickHouse/tests/integration
exec "$@"
