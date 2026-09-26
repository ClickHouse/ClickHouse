#!/bin/bash

set -e

export ZOO_SERVER_CNXN_FACTORY=org.apache.zookeeper.server.NettyServerCnxnFactory
export ZOO_SSL_KEYSTORE_LOCATION=/conf/certs/zookeeper.p12
export ZOO_SSL_KEYSTORE_PASSWORD=password
export ZOO_SSL_TRUSTSTORE_LOCATION=/conf/certs/truststore.p12
export ZOO_SSL_TRUSTSTORE_PASSWORD=password


# Allow the container to be started with `--user`
if [[ "$1" = 'zkServer.sh' && "$(id -u)" = '0' ]]; then
    chown -R zookeeper "$ZOO_DATA_DIR" "$ZOO_DATA_LOG_DIR" "$ZOO_LOG_DIR" "$ZOO_CONF_DIR"
    exec gosu zookeeper "$0" "$@"
fi

# Generate the config only if it doesn't exist
if [[ ! -f "$ZOO_CONF_DIR/zoo.cfg" ]]; then
    CONFIG="$ZOO_CONF_DIR/zoo.cfg"
    {
        echo "dataDir=$ZOO_DATA_DIR"
        echo "dataLogDir=$ZOO_DATA_LOG_DIR"

        echo "tickTime=$ZOO_TICK_TIME"
        echo "initLimit=$ZOO_INIT_LIMIT"
        echo "syncLimit=$ZOO_SYNC_LIMIT"

        echo "autopurge.snapRetainCount=$ZOO_AUTOPURGE_SNAPRETAINCOUNT"
        echo "autopurge.purgeInterval=$ZOO_AUTOPURGE_PURGEINTERVAL"
        echo "maxClientCnxns=$ZOO_MAX_CLIENT_CNXNS"
        echo "standaloneEnabled=$ZOO_STANDALONE_ENABLED"
        echo "admin.enableServer=$ZOO_ADMINSERVER_ENABLED"
    } >> "$CONFIG"
    if [[ -z $ZOO_SERVERS ]]; then
      ZOO_SERVERS="server.1=localhost:2888:3888;2181"
    fi

    for server in $ZOO_SERVERS; do
        echo "$server" >> "$CONFIG"
    done

    if [[ -n $ZOO_4LW_COMMANDS_WHITELIST ]]; then
        echo "4lw.commands.whitelist=$ZOO_4LW_COMMANDS_WHITELIST" >> "$CONFIG"
    fi


    if [[ -n $ZOO_SSL_QUORUM ]]; then
        {
            echo "sslQuorum=$ZOO_SSL_QUORUM"
            echo "serverCnxnFactory=$ZOO_SERVER_CNXN_FACTORY"
            echo "ssl.quorum.keyStore.location=$ZOO_SSL_QUORUM_KEYSTORE_LOCATION"
            echo "ssl.quorum.keyStore.password=$ZOO_SSL_QUORUM_KEYSTORE_PASSWORD"
            echo "ssl.quorum.trustStore.location=$ZOO_SSL_QUORUM_TRUSTSTORE_LOCATION"
            echo "ssl.quorum.trustStore.password=$ZOO_SSL_QUORUM_TRUSTSTORE_PASSWORD"
        } >> "$CONFIG"
    fi

    if [[ -n $ZOO_PORT_UNIFICATION ]]; then
        echo "portUnification=$ZOO_PORT_UNIFICATION" >> "$CONFIG"
    fi

    if [[ -n $ZOO_SECURE_CLIENT_PORT ]]; then
        {
            echo "secureClientPort=$ZOO_SECURE_CLIENT_PORT"
            echo "serverCnxnFactory=$ZOO_SERVER_CNXN_FACTORY"
            echo "ssl.keyStore.location=$ZOO_SSL_KEYSTORE_LOCATION"
            echo "ssl.keyStore.password=$ZOO_SSL_KEYSTORE_PASSWORD"
            echo "ssl.trustStore.location=$ZOO_SSL_TRUSTSTORE_LOCATION"
            echo "ssl.trustStore.password=$ZOO_SSL_TRUSTSTORE_PASSWORD"
        } >> "$CONFIG"
    fi

    if [[ -n $ZOO_CLIENT_PORT_UNIFICATION ]]; then
        echo "client.portUnification=$ZOO_CLIENT_PORT_UNIFICATION" >> "$CONFIG"
    fi
fi

# Write myid only if it doesn't exist
if [[ ! -f "$ZOO_DATA_DIR/myid" ]]; then
    echo "${ZOO_MY_ID:-1}" > "$ZOO_DATA_DIR/myid"
fi

mkdir -p "$(dirname $ZOO_SSL_KEYSTORE_LOCATION)"
mkdir -p "$(dirname $ZOO_SSL_TRUSTSTORE_LOCATION)"

if [[ ! -f "$ZOO_SSL_KEYSTORE_LOCATION" ]]; then
    keytool -genkeypair -alias zookeeper -keyalg RSA -validity 365 -keysize 2048 -dname "cn=zookeeper" -keypass password -keystore $ZOO_SSL_KEYSTORE_LOCATION -storepass password -deststoretype pkcs12
fi

if [[ ! -f "$ZOO_SSL_TRUSTSTORE_LOCATION" ]]; then
    keytool -importcert -alias zookeeper -file /clickhouse-config/client.crt -keystore $ZOO_SSL_TRUSTSTORE_LOCATION -storepass password -noprompt -deststoretype pkcs12
fi

exec "$@"
