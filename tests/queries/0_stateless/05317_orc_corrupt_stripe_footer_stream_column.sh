#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A 569-byte ORC file (id Int64, name String, v Float64, part Int32) whose stripe footer declares a stream for
# column 20, but the file has 5 columns. Remote reads prebuffer stripes (`orc::ReaderImpl::preBuffer`), which
# indexed the selected-column mask with that id and aborted the server. It must be reported as a malformed file
# instead. Local reads do not prebuffer, so the file is read through s3.
ORC_B64=$(tr -d '\n' <<'B64'
T1JDEQAACgYSBAgBUAArAAAKEwoDAAAAEgwIARIGCAIQAhgCUAAzAAAKFwoFAAAAAAASDggBIggKAWESAWEYAlAAUwAACicKAgAA
EiEIARobCQAAAAAAAPg/EQAAAAAAAPg/GQAAAAAAAPg/UAArAAAKEwoDAAAAEgwIARIGCAAQABgAUAAHAABCAIADAABhBwAAQACA
EQAAAAAAAAAA+D8HAABAAACuAAAotS/9IGR1AgCEAwoGCAYQABgLCgYIBhABGBgCGBwDGCwUARABGAYCGAQKBggCEAIDARAEGAYS
AggAEgIIAgASAggCCAB7/hAam+nkirydJ0AbQIZdZQm6AAAotS/9IF+lAgBEBApdCgQIAVAACg4IARIGCAIQAhgCUABYBgoQCAEi
CAoBYRIBYQoKIwgBGhsJAPg/EQD4PxkA+D9QAFgLABAAGABQAFgGBQBKyAIaAUJwd1jtfAJ4AQAotS/9IOydBQBCyiYskLc5////
///w6acgxiIELbemIDUZs4IjVzcRUBwT/pE0KZF9o02pZZNSphSQJ3okw0BbBghYiEDLlRuOvFiuU8aKKePElHHiyPYGVW7/bcVn
e6TQcm1Ni83htzOUa/ujDT2c/5Sg/4wO/hPqp6rnKrCqiyUKQ5//PDC42JMILQmsez1JVU0+1/o9wp3ZDoEFo2mF6HyRLSE6DwgA
SsgCGgFCcHdas32g06DT47p5Bgi/ARAFGICAECICAAwoYDAJgvQDA09SQxg=
B64
)
FILE="05317_${CLICKHOUSE_DATABASE}.orc"

$CLICKHOUSE_CLIENT --s3_truncate_on_insert 1 \
    --query "INSERT INTO FUNCTION s3(s3_conn, filename='$FILE', format='RawBLOB') SELECT base64Decode('$ORC_B64')"

$CLICKHOUSE_CLIENT --query "SELECT * FROM s3(s3_conn, filename='$FILE', format='ORC') FORMAT Null" 2>&1 \
    | grep -o -m1 'column=20 is out of range, the file has 5 columns'

# The server is still alive, and a valid ORC file is still read through the same path.
VALID="05317_valid_${CLICKHOUSE_DATABASE}.orc"
$CLICKHOUSE_CLIENT --s3_truncate_on_insert 1 \
    --query "INSERT INTO FUNCTION s3(s3_conn, filename='$VALID', format='ORC') SELECT number AS id, toString(number) AS s FROM numbers(100)"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(id) FROM s3(s3_conn, filename='$VALID', format='ORC')"
