#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `s3` table function and `BACKUP TO S3` are not compiled into the fast-test build.
#
# A named collection name is a bare identifier, so it cannot be uniquified from a `.sql` test.
# All globally-scoped names are qualified with $CLICKHOUSE_DATABASE so that parallel invocations
# of this test do not collide.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
NC="nc_agg_state_${DB}"
TABLE="t_agg_state_${DB}"

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION IF EXISTS ${NC};
CREATE NAMED COLLECTION ${NC} AS url = 'http://127.0.0.1:1/a.csv', format = 'CSV';
DROP TABLE IF EXISTS ${TABLE};
CREATE TABLE ${TABLE} (a UInt8) ENGINE = Memory;
"

# An aggregate function state has no textual representation, so it cannot be stored in the collection.
echo 'aggregate state override is rejected'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM s3(${NC}, filename = initializeAggregation('uniqState', 1))" 2>&1 |
    grep -o -m1 "Cannot use a value of type AggregateFunctionState as the override for key 'filename'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url(${NC}, format = initializeAggregation('uniqState', 1))" 2>&1 |
    grep -o -m1 "Cannot use a value of type AggregateFunctionState as the override for key 'format'"

echo 'aggregate state override is rejected for BACKUP'
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${TABLE} TO S3(${NC}, filename = initializeAggregation('uniqState', 1))" 2>&1 |
    grep -o -m1 "Cannot use a value of type AggregateFunctionState as the override for key 'filename'"

echo 'the error is BAD_ARGUMENTS, not an assertion'
${CLICKHOUSE_CLIENT} -m --query "
SELECT * FROM s3(${NC}, filename = initializeAggregation('uniqState', 1)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url(${NC}, format = initializeAggregation('uniqState', 1)); -- { serverError BAD_ARGUMENTS }
BACKUP TABLE ${TABLE} TO S3(${NC}, filename = initializeAggregation('uniqState', 1)); -- { serverError BAD_ARGUMENTS }
"

# The `format` override is echoed back by the UNKNOWN_FORMAT error, which pins the stored string:
# every value below must keep reaching the format factory unquoted.
echo 'other value types are still accepted and stored verbatim'
for value in "'NoSuchFormat1'" "42" "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')" "toIPv4('1.2.3.4')" "toIPv6('::1')" "toDecimal64(1.5, 2)" "toInt128(-170141183460469231731687303715884105728)" "toUInt256(255)" "1.5" "true"
do
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM url(${NC}, format = ${value})" 2>&1 |
        grep -o -m1 'Unknown format .*(UNKNOWN_FORMAT)'
done

# Containers are rendered by FieldVisitorToString, which handles an aggregate state, so a nested
# state must still be accepted.
echo 'an aggregate state nested in a container is still accepted'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url(${NC}, format = [initializeAggregation('uniqState', 1)])" 2>&1 |
    grep -c -m1 'UNKNOWN_FORMAT'

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE ${TABLE};
DROP NAMED COLLECTION ${NC};
"
