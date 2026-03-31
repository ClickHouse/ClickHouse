#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.arrow

# Generate a strictly compliant RFC 4122 Arrow file
python3 -c "
import pyarrow as pa
import pyarrow.feather as feather
import uuid

# python's uuid module automatically outputs Big-Endian network bytes
target_uuid = uuid.UUID('4d8f1ec8-bc41-4148-8069-d31c399b507b')

# Create a FixedSizeBinary(16) Arrow array
arr = pa.array([target_uuid.bytes], type=pa.binary(16))
table = pa.Table.from_arrays([arr], names=['u'])

feather.write_feather(table, '$DATA_FILE', compression='uncompressed')
"

$CLICKHOUSE_LOCAL -q "
    DROP TABLE IF EXISTS test_arrow_import;
    CREATE TABLE test_arrow_import (u UUID) ENGINE = Memory;
    
    -- When reading Arrow, ClickHouse will trigger our ArrowColumnToCHColumn C++ code
    INSERT INTO test_arrow_import FROM INFILE '$DATA_FILE' FORMAT Arrow;
    
    SELECT u FROM test_arrow_import;
"

rm -f $DATA_FILE