#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -m -q "
CREATE TABLE tx (c0 Tuple(Map(Variant(DateTime),Variant(Decimal)))) ENGINE = Memory();
INSERT INTO TABLE tx (c0) FROM INFILE '$CURDIR/data_binary/bad_variant_data.bin' FORMAT RowBinary; -- {clientError INCORRECT_DATA} 
DROP TABLE tx;
"
