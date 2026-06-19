#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet` format is not supported in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/04141_parquet_variant_value_only_wrapper.parquet"

# The fixture has a nested shredded `VARIANT` field with `value` but no `typed_value`.
cat <<'EOF' | base64 --decode | gunzip > "$FILE"
H4sICHRj7mkAA2R1Y2tkYl92YWx1ZV9vbmx5X3ZhcmlhbnQucGFycXVldAB9U91qE0EUPmd2moYm
lgZ7hlnIxVLJsi0a0tRGC4vYEsTLUnrhRaGd/QkW2yo2WRTyGF6LD+SFeN1HEPEJxLMbMbPbVJiZ
PT/fd352zhzuH20T0HMaPiTJQo1qAN5UAIDEZT5bAlCYqJGLkkVHmij+wMgmrVmU2vqMcpfDYb1N
nuVYfVw4WAfY4s32Pg0sgHs2AyCfsMJboANL6zjD3pnlK9KG5Rlpw579B/AbIWMuknBvdkHfSybx
myQ6vY5fp5eGBOwKjRnVTpre/SJFswO6fpmOTWLGJleFXsrMxSTNkY3xx3dpclronCXnGpIlFIlO
GdfZKHAR56pEw7jKhQoXlHTb7tSHNjVdAjfA7F9tJJRUkRr59VA3rDvSy3/vTkGw0N5qcZ92yFn2
Il5X9f2rcCBtxEvM7JrQVPBfcE4QCwml35bTnqkD/zuGWuYToItZ4GotrVxjNWBkVXCsXvmfRKhX
5mOiMY9WNvw/YFxp6YeYtwQLCbda+oXqhf/ZCTUiLyWC4lukVT+lkn5d3TgQ9IY8f8MDL0vfX5+/
vfKy7e6g23uUpFl/70nPC6LJ+UXiJU930mRnNIj3Nt1pG+D2gm88+Yf8iv8AEHmofMoDAAA=
EOF

"${CLICKHOUSE_LOCAL}" --multiquery --query "
SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET input_format_parquet_enable_json_parsing = 1;

SELECT c.name, c.path
FROM file('${FILE}', ParquetMetadata)
ARRAY JOIN columns AS c
WHERE startsWith(c.path, 'v.typed_value.b')
ORDER BY c.path
FORMAT TSVRaw;

SELECT toTypeName(v), toString(v)
FROM file('${FILE}', Parquet, 'v JSON')
ORDER BY toString(v)
FORMAT TSVRaw;

SET input_format_parquet_enable_json_parsing = 0;

SELECT toTypeName(v), dynamicType(v), toString(v)
FROM file('${FILE}', Parquet, 'v Dynamic')
ORDER BY toString(v)
FORMAT TSVRaw;
"
