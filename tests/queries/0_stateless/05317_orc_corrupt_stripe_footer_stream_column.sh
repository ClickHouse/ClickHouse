#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A 569-byte ORC file (id Int64, name String, v Float64, part Int32) with one row, whose stripe footer declares a
# row index stream for column 20, but the file has 5 columns. Two readers indexed the selected-column mask with
# that id and aborted the server; both must report a malformed file instead:
# - remote reads prebuffer stripes (`orc::ReaderImpl::preBuffer`), so the file is read through s3;
# - predicate pushdown loads the row indexes (`orc::RowReaderImpl::loadStripeIndex`), also for local files. The
#   predicate must keep the stripe (`id >= 0`; the row has id 1), or the stripe statistics skip it first.
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

# Stripes are prebuffered only with `remote_filesystem_read_prefetch`, which CI randomizes.
$CLICKHOUSE_CLIENT --remote_filesystem_read_prefetch 1 \
    --query "SELECT * FROM s3(s3_conn, filename='$FILE', format='ORC') FORMAT Null" 2>&1 \
    | grep -o -m1 'column=20 is out of range, the file has 5 columns'

# Predicate pushdown (on by default) on a local file, which is never prebuffered.
LOCAL_FILE="05317_${CLICKHOUSE_DATABASE}.orc"
$CLICKHOUSE_CLIENT --engine_file_truncate_on_insert 1 \
    --query "INSERT INTO FUNCTION file('$LOCAL_FILE', 'RawBLOB') SELECT base64Decode('$ORC_B64')"
$CLICKHOUSE_CLIENT --input_format_orc_filter_push_down 1 \
    --query "SELECT * FROM file('$LOCAL_FILE', 'ORC') WHERE id >= 0 FORMAT Null" 2>&1 \
    | grep -o -m1 'column=20 is out of range, the file has 5 columns'
rm -f "${USER_FILES_PATH:?}/$LOCAL_FILE"

# The server is still alive, and a valid ORC file is still read through the same path.
VALID="05317_valid_${CLICKHOUSE_DATABASE}.orc"
$CLICKHOUSE_CLIENT --s3_truncate_on_insert 1 \
    --query "INSERT INTO FUNCTION s3(s3_conn, filename='$VALID', format='ORC') SELECT number AS id, toString(number) AS s FROM numbers(100)"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(id) FROM s3(s3_conn, filename='$VALID', format='ORC')"
