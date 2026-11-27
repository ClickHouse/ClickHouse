#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ASSEMBLYSCRIPT` builds one object per row with `createString` and never reads
# `serialization_format`, so the input it places in guest memory is not a serialized block and
# must not be bounded by the size of one. A row of 100000 zero bytes is about 600 KB of
# `JSONEachRow`, which would be refused under a 576 KiB cap, while the call itself only allocates
# the `4 * bytes` upper bound of a single AssemblyScript string.

MODULE="as_string_${CLICKHOUSE_DATABASE}"
FUNC="wasm_as_string_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/as_example.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --query "
CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM '${MODULE}' :: 'str_length'
    ARGUMENTS (s String) RETURNS UInt32
    SETTINGS serialization_format = 'JSONEachRow'"

echo 'a row the ignored serialization format would refuse is still passed to the guest'
${CLICKHOUSE_CLIENT} --query "
SELECT ${FUNC}(unhex(repeat('00', 100000))) SETTINGS webassembly_udf_max_memory = 589824"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
