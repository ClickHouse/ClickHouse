#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Arrow format is not available in fast test builds

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/65036
#
# During format auto-detection, the ArrowStream format reader would interpret
# the first bytes of non-Arrow data (e.g. JSON) as a metadata length in the
# Arrow IPC framing protocol. For example, JSON starting with "{\n  " was
# interpreted as a ~514 MiB metadata length, causing a huge allocation before
# Arrow discovered the data was invalid. This test verifies that format
# detection on a JSON file without a file extension does not cause excessive
# memory usage.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create a JSON file without extension to trigger format auto-detection.
DATA_FILE="${CLICKHOUSE_TMP}/test_${CLICKHOUSE_DATABASE}_json_no_ext"
cat > "${DATA_FILE}" <<'EOF'
[{"a": 1, "b": "hello"}, {"a": 2, "b": "world"}]
EOF

# Run the query and check that peak memory usage is reasonable.
# Before the fix, this would allocate ~514 MiB during ArrowStream format detection.
${CLICKHOUSE_LOCAL} --query "
    SELECT *
    FROM file('${DATA_FILE}')
    ORDER BY a
    SETTINGS max_memory_usage = '100Mi'
"

rm -f "${DATA_FILE}"
