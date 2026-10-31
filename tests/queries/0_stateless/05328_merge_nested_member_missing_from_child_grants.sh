#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A Nested member that a Merge child does not have is sized only from a sibling the user can select
# from that child: without such a grant it reads as `[]`, never as ACCESS_DENIED.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE t_c (k UInt8, \`n.a\` Array(UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_c VALUES (1, [1, 2]);
CREATE TABLE t_d AS t_c ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_c);
CREATE TABLE t_b AS t_c ENGINE = Buffer(currentDatabase(), t_c, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);
CREATE TABLE t_mi (k UInt8, \`n.a\` Array(UInt8)) ENGINE = Merge(currentDatabase(), '^t_c\$');
CREATE TABLE t_al ENGINE = Alias('t_c');
CREATE TABLE m_c (k UInt8, \`n.a\` Array(UInt8), \`n.b\` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_c\$');
CREATE TABLE m_d (k UInt8, \`n.a\` Array(UInt8), \`n.b\` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_d\$');
CREATE TABLE m_b (k UInt8, \`n.a\` Array(UInt8), \`n.b\` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_b\$');
CREATE TABLE m_mi (k UInt8, \`n.a\` Array(UInt8), \`n.b\` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_mi\$');
CREATE TABLE m_al (k UInt8, \`n.a\` Array(UInt8), \`n.b\` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_al\$');
SET enable_quantized_codec = 1;
CREATE TABLE m_bq (k UInt8, \`n.a\` Array(UInt8), \`n.q\` Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = Merge(currentDatabase(), '^t_b\$');
"

for t in t_c t_d t_b t_mi t_al m_c m_d m_b m_mi m_al; do
    ${CLICKHOUSE_CLIENT} --query "GRANT SELECT(k, \`n.b\`) ON ${CLICKHOUSE_DATABASE}.${t} TO ${user}"
done

grant_n_a()
{
    for t in "$@"; do
        ${CLICKHOUSE_CLIENT} --query "GRANT SELECT(\`n.a\`) ON ${CLICKHOUSE_DATABASE}.${t} TO ${user}"
    done
}

run()
{
    for t in "$@"; do
        echo "$t $(${CLICKHOUSE_CLIENT} --user "${user}" --password password --query "SELECT n.b FROM ${t}" 2>&1 | grep -o -m1 -E '^\[.*\]$|ACCESS_DENIED')"
    done
}

echo "without a grant on n.a"
run m_c m_d m_b m_mi m_al

echo "with a grant on n.a of the MergeTree table only"
grant_n_a t_c
run m_c m_al

echo "with a grant on n.a of the Alias only"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(\`n.a\`) ON ${CLICKHOUSE_DATABASE}.t_c FROM ${user}"
grant_n_a t_al
run m_al

echo "codes of a Quantized member through a Buffer, with n.a granted on the Buffer only"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_b TO ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.m_bq TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --password password --query "SELECT hex(n.q.quantized) FROM m_bq" 2>&1 | grep -o -m1 -E '^[0-9A-F]+$|ACCESS_DENIED'

echo "with a grant on n.a of every table"
grant_n_a t_c t_d t_b t_mi
run m_c m_d m_b m_mi m_al

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
