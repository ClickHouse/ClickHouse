#!/usr/bin/env bash
# Tags: no-fasttest

# A path in Iceberg metadata is not required to be a URI, and an object key may legitimately
# contain a colon - an unescaped partition value, say. `SchemeAuthorityKey` used to read the text
# before any colon that is followed by '/' as a URI scheme (the RFC 8089 `file:/path` form), so a
# table whose directory segment ends with ':' had every one of its file paths decomposed into a
# nonsense scheme and a truncated key, and reading it failed with
# `Unsupported storage scheme '<the directory>' in path '<the file>'`.
#
# The table below writes scheme-less absolute paths into its metadata
# (`write_full_path_in_iceberg_metadata = 0`, the default), so the manifest entries read
# `<work dir>/t0:/data/<uuid>.parquet` and go through exactly that branch.
#
# It runs under `clickhouse local`, whose path prefix is the root, because a server resolves the
# engine argument against `user_files_path`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/iceberg_colon_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
# The trailing ':' is the point of the test: it makes the table directory look like a URI scheme.
TABLE_DIR="${WORK_DIR}/t0:"
mkdir -p "${TABLE_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

${CLICKHOUSE_LOCAL} \
    --allow_insert_into_iceberg=1 \
    --multiquery -q "
CREATE TABLE t0 (c0 Int) ENGINE = IcebergLocal('${TABLE_DIR}/');
INSERT INTO t0 VALUES (42);
SELECT c0 FROM t0;
"
