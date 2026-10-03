#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-fasttest: an S3 disk
# no-shared-merge-tree: custom disk

# `mergeTreeParts` reads from the disk described by the query, so it requires the `READ` grant on
# the source of that disk - `FILE` for a local disk, `S3` for an S3 disk - on every path that resolves
# the structure or reads: `SELECT`, `DESCRIBE` and `CREATE TABLE ... AS`. A filtered grant is matched
# against the literal `endpoint` of the disk description.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_mtp_${CLICKHOUSE_DATABASE}"
DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_grants_${CLICKHOUSE_DATABASE}/"
S3_ROOT="http://localhost:11111/test/mtp_grants_${CLICKHOUSE_DATABASE}/"
S3_CREDENTIALS="access_key_id = 'test', secret_access_key = 'testtest'"

function mtp()
{
    echo "mergeTreeParts(structure('id Int64'), parts(), disk($1), table_settings(index_granularity_bytes = 10485760))"
}

function denied()
{
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${USER}" --query "$1" 2>&1)
    echo "${output}" | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "UNEXPECTED: ${output}"
}

function allowed()
{
    ${CLICKHOUSE_CLIENT} --user "${USER}" --query "$1" 2>&1 | cut -f 1,2
}

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${USER};
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${USER};"

echo "-- without a grant on the source"
denied "SELECT count() FROM $(mtp "type = local, path = '${DISK_ROOT}'")"
denied "DESCRIBE $(mtp "type = local, path = '${DISK_ROOT}'")"
denied "CREATE TABLE mtp_grants_denied AS $(mtp "type = local, path = '${DISK_ROOT}'")"
denied "SELECT count() FROM $(mtp "type = s3, endpoint = '${S3_ROOT}allowed/', ${S3_CREDENTIALS}")"
test -d "${DISK_ROOT}" && echo "the disk was created for a query without the grant" || echo "no directory"

echo "-- READ ON FILE"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON FILE TO ${USER}"
allowed "SELECT count() FROM $(mtp "type = local, path = '${DISK_ROOT}'")"
allowed "DESCRIBE $(mtp "type = local, path = '${DISK_ROOT}'")"
denied "SELECT count() FROM $(mtp "type = s3, endpoint = '${S3_ROOT}allowed/', ${S3_CREDENTIALS}")"
${CLICKHOUSE_CLIENT} --query "REVOKE READ ON FILE FROM ${USER}"

echo "-- READ ON S3 filtered to one prefix"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON S3('${S3_ROOT}allowed/.*') TO ${USER}"
allowed "SELECT count() FROM $(mtp "type = s3, endpoint = '${S3_ROOT}allowed/', ${S3_CREDENTIALS}")"
allowed "DESCRIBE $(mtp "type = s3, endpoint = '${S3_ROOT}allowed/', ${S3_CREDENTIALS}")"
denied "SELECT count() FROM $(mtp "type = s3, endpoint = '${S3_ROOT}other/', ${S3_CREDENTIALS}")"
denied "DESCRIBE $(mtp "type = s3, endpoint = '${S3_ROOT}other/', ${S3_CREDENTIALS}")"
denied "CREATE TABLE mtp_grants_denied AS $(mtp "type = s3, endpoint = '${S3_ROOT}other/', ${S3_CREDENTIALS}")"
# An `endpoint_subpath` moves the data below the endpoint, so the endpoint alone is not what the
# filter is matched against; the query then needs the unfiltered grant.
denied "SELECT count() FROM $(mtp "type = s3, endpoint = '${S3_ROOT}allowed/', endpoint_subpath = 'sub/', ${S3_CREDENTIALS}")"
denied "SELECT count() FROM $(mtp "type = local, path = '${DISK_ROOT}'")"

echo "-- READ ON S3 without a filter"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON S3 TO ${USER}"
allowed "SELECT count() FROM $(mtp "type = s3, endpoint = '${S3_ROOT}other/', ${S3_CREDENTIALS}")"
allowed "SELECT count() FROM $(mtp "type = s3, endpoint = '${S3_ROOT}allowed/', endpoint_subpath = 'sub/', ${S3_CREDENTIALS}")"

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER}"
