#!/usr/bin/env bash
set -e

source default-config

SERVER_BIN="${WORKSPACE}/build/dbms/src/Server/clickhouse"
SERVER_CONF="${WORKSPACE}/sources/dbms/src/Server/config.xml"
SERVER_DATADIR="${WORKSPACE}/clickhouse"

[[ -x "$SERVER_BIN" ]] || die "Run build-normal.sh first"
[[ -r "$SERVER_CONF" ]] || die "Run get-sources.sh first"

mkdir "${SERVER_DATADIR}"

$SERVER_BIN server --config-file "$SERVER_CONF" --pid-file="${WORKSPACE}/clickhouse.pid" --path "$SERVER_DATADIR" --daemon
