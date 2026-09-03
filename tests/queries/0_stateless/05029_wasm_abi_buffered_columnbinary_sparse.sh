#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# `system.webassembly_modules` is a single server-wide registry, not per-database, so a
# fixed module name collides when the same test runs concurrently with itself (the flaky
# check does exactly that): one copy's `DELETE FROM system.webassembly_modules` removes
# the module another copy just inserted. Derive the name from the per-test database so
# each run owns its own entry, instead of serializing the test with `no-parallel`.
MODULE="columnar_abi_sparse_${CLICKHOUSE_DATABASE}"
# User-defined functions share the same server-wide namespace, so uniquify the name too.
FUNC="sparse_byte_sum_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} << EOF

CREATE OR REPLACE FUNCTION ${FUNC}
    LANGUAGE WASM ABI BUFFERED_V1
    FROM '${MODULE}' :: 'add_offset_col'
    ARGUMENTS (s String, n UInt64) RETURNS UInt64
    SETTINGS serialization_format = 'ColumnBinary';

DROP TABLE IF EXISTS t_columnbinary_sparse;

-- \`ratio_of_defaults_for_sparse_serialization = 0\` forces sparse serialization for
-- every column, so the columns reaching the format are \`ColumnSparse\` wrappers.
CREATE TABLE t_columnbinary_sparse (id UInt64, s String, n UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0;

INSERT INTO t_columnbinary_sparse SELECT number, '', 0 FROM numbers(20);
INSERT INTO t_columnbinary_sparse VALUES (100, 'abc', 250);

SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_columnbinary_sparse' AND column = 's'
ORDER BY serialization_kind;

-- A sparse \`String\` argument must reach the guest correctly. The precompute
-- (preallocation) pass in \`ColumnBinaryOutputFormat\` runs before \`consume\`
-- normalizes the wrappers, so it must strip \`ColumnSparse\` itself; otherwise
-- \`buildColDescriptor\` misses the \`ColumnString\` branch and throws.
SELECT sum(${FUNC}(s, n)) FROM t_columnbinary_sparse;

-- Fixed-width sparse columns are sized by \`sizeOfValueIfFixed\`, which reports a
-- wider value for \`ColumnSparse\`; check a fixed-width-only signature too.
SELECT sum(${FUNC}('', n)) FROM t_columnbinary_sparse;

DROP TABLE t_columnbinary_sparse;
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
