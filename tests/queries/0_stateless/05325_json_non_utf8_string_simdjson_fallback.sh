#!/usr/bin/env bash
# Tags: no-fasttest, use-simdjson
# no-fasttest: the Fast test build has no simdjson

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# simdjson's scalar `fallback` backend accepts `String` bytes that are not valid UTF-8, like its SIMD backends do.
# Runs only where SIMDJSON_FORCE_IMPLEMENTATION can select `fallback`: it must be compiled in, and simdjson must read the
# variable at all (with a single compiled-in backend it ignores it, so even an unknown name would parse).
probe() { SIMDJSON_FORCE_IMPLEMENTATION=$1 $CLICKHOUSE_LOCAL --allow_simdjson 1 -q "SELECT isValidJSON('{}')" 2>&1; }
if [ "$(probe fallback)" != "1" ] || [ "$(probe no_such_backend)" = "1" ]; then
    echo "@@SKIP@@: this build cannot select the simdjson fallback backend"
    exit 0
fi

SIMDJSON_FORCE_IMPLEMENTATION=fallback $CLICKHOUSE_LOCAL --allow_simdjson 1 <<'EOF'
SELECT hex(CAST(tuple(unhex('e0a4'))::Tuple(test String) AS JSON)::String);
SELECT x, hex(JSONExtractString(doc, 'a')), isValidJSON(doc)
FROM (SELECT arrayJoin(['e0a4', 'e0a4b9']) AS x, concat('{"a":"', unhex(x), '"}') AS doc);
SELECT isValidJSON('{"a"invalid}'), isValidJSON(concat('{"a":"', unhex('e0a4')));
EOF
