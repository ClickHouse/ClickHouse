#!/usr/bin/env bash
# `remote` to an address of this server that is not classified local (e.g. `127.0.0.2`) must check the caller's
# own grants, like a local shard: the connection is answered by this server, and its peer address `127.0.0.1`
# satisfies the `HOST LOCAL` restriction of the account given to `remote`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

svc="svc_${CLICKHOUSE_DATABASE}"
bob="bob_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"
port="${CLICKHOUSE_PORT_TCP}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${svc}, ${bob};
    CREATE TABLE ${db}.secret (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${db}.secret VALUES (1, 'secret');
    CREATE USER ${svc} NOT IDENTIFIED HOST LOCAL;
    GRANT SELECT, INSERT ON ${db}.secret TO ${svc};
    CREATE USER ${bob} IDENTIFIED WITH plaintext_password BY 'bob';
    GRANT REMOTE ON *.* TO ${bob};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${bob};
"

function as_bob()
{
    local query="$1"
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${bob}" --password bob --query "${query}" 2>&1)
    if grep -q "ACCESS_DENIED" <<< "${output}"; then
        echo "ACCESS_DENIED"
    else
        echo "${output}"
    fi
}

for host in "127.0.0.2:${port}" "127.0.0.1:${port}" "0.0.0.0:${port}" "[::ffff:127.0.0.2]:${port}" "[::]:${port}" "localhost:${port}"; do
    echo "--- ${host}"
    as_bob "SELECT s FROM remote('${host}', ${db}, secret, '${svc}', '')"
    as_bob "INSERT INTO FUNCTION remote('${host}', ${db}, secret, '${svc}', '') VALUES (2, 'forged')"
    as_bob "DESC remote('${host}', ${db}, secret, '${svc}', '')"
    as_bob "SELECT s FROM remote('${host}', view(SELECT s FROM ${db}.secret), '${svc}', '')"
done

${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE, DROP TABLE ON ${db}.* TO ${bob}"

echo "--- table function with the structure given explicitly"
as_bob "CREATE TABLE ${db}.cached (s String) AS remote('127.0.0.2:${port}', view(SELECT s FROM ${db}.secret), '${svc}', '')"
as_bob "SELECT s FROM ${db}.cached"
as_bob "DROP TABLE IF EXISTS ${db}.cached"

echo "--- Remote engine"
as_bob "CREATE TABLE ${db}.proxy ENGINE = Remote('127.0.0.2:${port}', ${db}, secret, '${svc}', '')"
as_bob "CREATE TABLE ${db}.proxy (id UInt32, s String) ENGINE = Remote('127.0.0.2:${port}', ${db}, secret, '${svc}', '')"
as_bob "CREATE TABLE ${db}.proxy (s String) ENGINE = Remote('127.0.0.2:${port}', merge('${db}', '^secret\$'), '${svc}', '')"

echo "--- Remote database engine"
proxy_db="${CLICKHOUSE_DATABASE}_proxy"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${proxy_db} ENGINE = Remote('127.0.0.2:${port}', '${db}', '${svc}', '')"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON ${proxy_db}.* TO ${bob}"
as_bob "SELECT s FROM ${proxy_db}.secret"
as_bob "INSERT INTO ${proxy_db}.secret VALUES (4, 'forged')"

echo "--- an unrelated table function target is allowed"
as_bob "SELECT * FROM remote('127.0.0.2:${port}', numbers(3), '${svc}', '')"

echo "--- with grants"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON ${db}.secret TO ${bob}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.proxy TO ${bob}"
as_bob "SELECT s FROM remote('127.0.0.2:${port}', ${db}, secret, '${svc}', '')"
as_bob "INSERT INTO FUNCTION remote('127.0.0.2:${port}', ${db}, secret, '${svc}', '') VALUES (3, 'granted')"
as_bob "SELECT s FROM remote('127.0.0.2:${port}', view(SELECT s FROM ${db}.secret), '${svc}', '') ORDER BY s"
as_bob "CREATE TABLE ${db}.proxy ENGINE = Remote('127.0.0.2:${port}', ${db}, secret, '${svc}', '')"
as_bob "SELECT s FROM ${db}.proxy ORDER BY s"

as_bob "SELECT s FROM ${proxy_db}.secret ORDER BY s"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${proxy_db}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${svc}, ${bob}"
