#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet` format is not supported in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIXTURE_FILE="${CLICKHOUSE_TMP}/04310_parquet_variant_uuid_external.parquet"
WRITER_FILE="${CLICKHOUSE_TMP}/04310_parquet_variant_uuid_writer.parquet"

rm -f "$FIXTURE_FILE" "$WRITER_FILE"

# The fixture is an unshredded `Parquet` `VARIANT` value written with PyArrow
# and patched to carry the `VARIANT` logical type. Its `UUID` payload bytes are
# RFC/network order, not ClickHouse raw storage order.
cat <<'EOF' | base64 --decode | gunzip > "$FIXTURE_FILE"
H4sIAAAAAAACA5VSzU7CQBCeLtpw4CAx23RND0RjwwERSSTR4GFqEIx/SASi4VKhARMV5Kf4CiY+gO+gB8++g2/gO/gGxtm2KSAn
J9vZ2Z3v+2Zm0zJWtjjwFb6S4owClatGDpIRBUCXLh4HACY/JUKebgiU47kZdLysrBXaT6+f3z9fHy/uRXZJn7+aUpLRXJ4viONt
0NVBs+Pc2ZzBNtMVly80YollKgs8tg569M4Z2i17aPvHRde+HTmgMWGIlAkGj4l1FURScUMgp6zW0BpmNP9nLmFwOQyDvKYKVZhe
czMivryn8K7QMlv5f0w7pb8a6mvPTGNmlDbK6zGsVM7qu/7I+hvblLbTRrJz9O0IcYxWGw8Q27jvpdCyEE9ksI946F1MdrKCdNUg
qkoe8UtSx9O1AmUsnBY90jgkodQvjCf1pS9dykpN2Vq2TM5Pe/kiTrdKUh7+qnNdqt3K86PEP6Js/jAodRIUC0mWFVaetBbgr+u1
zGW90mkV/fasCXUP9ETP7j+MnOFGs9fbsPv97jjhOv3BTfc+kc2mM+mMSBkAtKBBT1+mP/0X0q41ue4CAAA=
EOF

"${CLICKHOUSE_LOCAL}" --multiquery --query "
SET allow_experimental_dynamic_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

SELECT dynamicType(v), toString(v)
FROM file('${FIXTURE_FILE}', Parquet, 'v Dynamic')
FORMAT TSVRaw;

SELECT v.UUID
FROM file('${FIXTURE_FILE}', Parquet, 'v Dynamic')
FORMAT TSVRaw;

SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_compression_method = 'none';

CREATE TABLE src
(
    v Dynamic
)
ENGINE = Memory;

INSERT INTO src SELECT CAST(toUUID('01234567-89ab-cdef-fedc-ba9876543210'), 'Dynamic');
INSERT INTO src SELECT CAST(toInt64(42), 'Dynamic');

SELECT *
FROM src
INTO OUTFILE '${WRITER_FILE}'
FORMAT Parquet;
"

python3 - "$WRITER_FILE" <<'PY'
import sys
from pathlib import Path

data = Path(sys.argv[1]).read_bytes()
expected = bytes.fromhex("500123456789abcdeffedcba9876543210")
old_little_endian_payload = bytes.fromhex("50efcdab89674523011032547698badcfe")

if expected not in data:
    raise RuntimeError("missing big-endian `UUID` payload in writer output")
if old_little_endian_payload in data:
    raise RuntimeError("found old raw-order `UUID` payload in writer output")

print("writer `UUID` payload is big-endian")
PY
