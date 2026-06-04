#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -m -q "
CREATE TABLE t (v Variant(String, UInt64)) ENGINE = Memory();
INSERT INTO TABLE t (v) FROM INFILE '$CUR_DIR/data_binary/bad_variant_discriminator.native' FORMAT Native; -- {clientError INCORRECT_DATA}
DROP TABLE t;
"
