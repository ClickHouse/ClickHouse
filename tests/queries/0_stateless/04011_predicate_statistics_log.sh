#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

# Verify that system.predicate_statistics_log collects per-predicate selectivity data
# The global predicate_statistics_sample_rate is 0 to avoid overhead in unrelated tests
# this test enables it temporarily via a config override + SYSTEM RELOAD CONFIG

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config_override="${CLICKHOUSE_CONFIG_DIR}/config.d/zzz_predicate_statistics_test_override.xml"

# enable predicate statistics collection for this test
cat > "$config_override" <<'EOF'
<clickhouse>
    <predicate_statistics_sample_rate>1</predicate_statistics_sample_rate>
</clickhouse>
EOF

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG"

# on exit: disable the override and reload config
trap 'rm -f "$config_override"; $CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" 2>/dev/null' EXIT

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test_pred_stats;

CREATE TABLE test_pred_stats (id UInt64, status String, value Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_pred_stats SELECT number, if(number % 10 = 0, 'active', 'inactive'), rand() FROM numbers(100000);

-- Equality predicate
SELECT count() FROM test_pred_stats WHERE status = 'active' FORMAT Null;
-- Range predicate
SELECT count() FROM test_pred_stats WHERE id > 50000 FORMAT Null;
-- In predicate
SELECT count() FROM test_pred_stats WHERE id IN (1, 2, 3) FORMAT Null;

SYSTEM FLUSH LOGS predicate_statistics_log;

SELECT
    column_name,
    predicate_class,
    function_name,
    sum(input_rows) > 0 AS has_input,
    sum(passed_rows) > 0 AS has_output
FROM system.predicate_statistics_log
WHERE table = 'test_pred_stats' AND currentDatabase() = database
GROUP BY column_name, predicate_class, function_name
ORDER BY column_name, predicate_class, function_name;

DROP TABLE test_pred_stats;
"
