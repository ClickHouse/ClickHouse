#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Addresses are expanded before anything is connected to, so nothing here reaches the network.
# The `remote` family shares one parser across several surfaces, and each of them has to report
# itself in the error: `remoteSecure` must not call itself `remote`, and the same for the
# `Remote`/`RemoteSecure` table and database engines.

$CLICKHOUSE_CLIENT --query "SELECT * FROM remoteSecure('127.0.0.{1..2000}', system.one)" 2>&1 \
    | grep -oF -e "Table function 'remoteSecure'" -e "too many result addresses: 2000, while at most 1000 are allowed" -e "'table_function_remote_max_addresses' setting" \
    | head -n 3

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.remote_glob (x UInt8) ENGINE = Remote('127.0.0.{1..2000}', '${CLICKHOUSE_DATABASE}', 'remote_glob_target')" 2>&1 \
    | grep -oF -e "Table engine 'Remote'" -e "too many result addresses: 2000, while at most 1000 are allowed" -e "'table_function_remote_max_addresses' setting" \
    | head -n 3

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.remote_glob (x UInt8) ENGINE = RemoteSecure('127.0.0.{1..2000}', '${CLICKHOUSE_DATABASE}', 'remote_glob_target')" 2>&1 \
    | grep -oF -e "Table engine 'RemoteSecure'" -e "too many result addresses: 2000, while at most 1000 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_remote_glob ENGINE = Remote('127.0.0.{1..2000}', 'default')" 2>&1 \
    | grep -oF -e "Database engine 'Remote'" -e "too many result addresses: 2000, while at most 1000 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_remote_glob ENGINE = RemoteSecure('127.0.0.{1..2000}', 'default')" 2>&1 \
    | grep -oF -e "Database engine 'RemoteSecure'" -e "too many result addresses: 2000, while at most 1000 are allowed" \
    | head -n 2
