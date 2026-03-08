#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} <<EOF
  CREATE TABLE test (
    uint8 UInt8,
    int8 Int8,
    uint16 UInt16,
    int16 Int16,
    uint32 UInt32,
    int32 Int32,
    uint64 UInt64,
    int64 Int64,
  );

  INSERT INTO test FROM INFILE '${CLICKHOUSE_TMP}/fixed.avro' FORMAT avro;

  SELECT * FROM test FORMAT CSV;
EOF
