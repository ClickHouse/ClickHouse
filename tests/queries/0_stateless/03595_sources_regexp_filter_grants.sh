#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user1="user03595_1_${CLICKHOUSE_DATABASE}_$RANDOM"
user2="user03595_2_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
-- Cleanup
DROP USER IF EXISTS $user1, $user2;
CREATE USER $user1;
CREATE USER $user2;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user1;
EOF

${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL('http://localhost:812[1-3]/.*') TO $user1 WITH GRANT OPTION";
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM url('http://localhost:8124/', LineAsString) FORMAT Null;" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM url('http://localhost:8123/', LineAsString) FORMAT Null;";

echo '--without grants--'
${CLICKHOUSE_CLIENT} --query "REVOKE READ ON URL FROM $user1";
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM url('http://localhost:8124/', LineAsString) FORMAT Null;" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM url('http://localhost:8123/', LineAsString) FORMAT Null;" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

echo '--multiple grants--'
${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL('http://localhost:912.*') TO $user1";
${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL('http://localhost:812.*') TO $user1";
${CLICKHOUSE_CLIENT} --query "GRANT READ ON S3('http://localhost:11111/.*') TO $user1";
${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM url('http://localhost:8123/', LineAsString) FORMAT Null;";
${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV') FORMAT Null;";
echo 'OK'

echo '--wrong grant--'
${CLICKHOUSE_CLIENT} --query "REVOKE READ ON URL FROM $user1";
${CLICKHOUSE_CLIENT} --query "GRANT WRITE ON URL('http://localhost:812.*') TO $user1";
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM url('http://localhost:8123/', LineAsString) FORMAT Null;" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

echo '--partial revokes--'
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM $user1";
${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL('http://localhost:812.*') TO $user1";
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TEMPORARY TABLE ON *.* TO $user1;";
${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM url('http://localhost:8123/', LineAsString) FORMAT Null;";
${CLICKHOUSE_CLIENT} --query "REVOKE READ ON URL('foo.*') FROM $user1";
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "SELECT * FROM url('http://localhost:8123/', LineAsString) FORMAT Null;" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

echo '--invalid regexp--'
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "GRANT READ ON URL('(\w+) \1') TO $user1;" 2>&1 | grep -c "Syntax error") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user $user1 --query "GRANT READ ON URL('(?Pempty_name)') TO $user1;" 2>&1 | grep -c "Syntax error") >= 1 )) && echo "OK" || echo "UNEXPECTED"


${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user1, $user2;
EOF
