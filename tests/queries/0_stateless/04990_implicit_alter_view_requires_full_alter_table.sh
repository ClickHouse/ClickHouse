#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ALTER VIEW` is implicitly enabled by the `ALTER TABLE` group as a whole, not by a single child
# of that group. So one child privilege such as `ALTER MODIFY COMMENT` must not confer
# `ALTER VIEW MODIFY QUERY`, while the full `ALTER TABLE` grant must. The same holds for a grant
# on a whole database.

src="t_src_${CLICKHOUSE_TEST_UNIQUE_NAME}"
dst="t_dst_${CLICKHOUSE_TEST_UNIQUE_NAME}"
mview="mv_${CLICKHOUSE_TEST_UNIQUE_NAME}"

user_partial="u_partial_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_none="u_none_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_full="u_full_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_wild="u_wild_${CLICKHOUSE_TEST_UNIQUE_NAME}"
users=("${user_partial}" "${user_none}" "${user_full}" "${user_wild}")

${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS ${mview}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${src}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${dst}"
for u in "${users[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${u}"
done

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${src} (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${dst} (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW ${mview} TO ${dst} AS SELECT a FROM ${src}"

# The only difference between the users is their ALTER grant.
for u in "${users[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "CREATE USER ${u}"
    ${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${src} TO ${u}"
    ${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${dst} TO ${u}"
done

${CLICKHOUSE_CLIENT} --query "GRANT ALTER MODIFY COMMENT ON ${mview} TO ${user_partial}"
${CLICKHOUSE_CLIENT} --query "GRANT ALTER TABLE ON ${mview} TO ${user_full}"
${CLICKHOUSE_CLIENT} --query "GRANT ALTER UPDATE ON ${CLICKHOUSE_DATABASE}.* TO ${user_wild}"

echo "one ALTER TABLE child on the view:"
err=$(${CLICKHOUSE_CLIENT} --user "${user_partial}" --query "ALTER TABLE ${mview} MODIFY QUERY SELECT a FROM ${src}" 2>&1)
echo "$err" | grep -o -m1 "ACCESS_DENIED"
echo "$err" | grep -o -m1 "ALTER VIEW MODIFY QUERY"

echo "no ALTER grant at all:"
${CLICKHOUSE_CLIENT} --user "${user_none}" --query "ALTER TABLE ${mview} MODIFY QUERY SELECT a FROM ${src}" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "whole ALTER TABLE group on the view:"
${CLICKHOUSE_CLIENT} --user "${user_full}" --query "ALTER TABLE ${mview} MODIFY QUERY SELECT a FROM ${src}" && echo "OK"

echo "one ALTER TABLE child on the database:"
${CLICKHOUSE_CLIENT} --user "${user_wild}" --query "ALTER TABLE ${mview} MODIFY QUERY SELECT a FROM ${src}" 2>&1 | grep -o -m1 "ACCESS_DENIED"

${CLICKHOUSE_CLIENT} --query "GRANT ALTER TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${user_wild}"

echo "whole ALTER TABLE group on the database:"
${CLICKHOUSE_CLIENT} --user "${user_wild}" --query "ALTER TABLE ${mview} MODIFY QUERY SELECT a FROM ${src}" && echo "OK"

echo "the granted privilege still works:"
${CLICKHOUSE_CLIENT} --user "${user_partial}" --query "ALTER TABLE ${mview} MODIFY COMMENT 'c'" && echo "OK"

${CLICKHOUSE_CLIENT} --query "DROP VIEW ${mview}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${src}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${dst}"
for u in "${users[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP USER ${u}"
done
