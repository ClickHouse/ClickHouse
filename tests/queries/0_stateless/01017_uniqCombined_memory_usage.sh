#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# each uniqCombined state should not use > sizeof(HLL) in memory,
# sizeof(HLL) is (2^K * 6 / 8)
# hence max_memory_usage for 100 rows = (96<<10)*100 = 9830400

# HashTable for UInt32 (used until (1<<13) elements), hence 8192 elements
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT 'UInt32';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 4000000;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 8192) AS k, uniqCombined(number % 8192) u FROM numbers(8192 * 100) GROUP BY k);" # { serverError 241 }
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 9830400;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 8192) AS k, uniqCombined(number % 8192) u FROM numbers(8192 * 100) GROUP BY k);"

# HashTable for UInt64 (used until (1<<12) elements), hence 4096 elements
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT 'UInt64';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 4000000;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 4096) AS k, uniqCombined(reinterpretAsString(number % 4096)) u FROM numbers(4096 * 100) GROUP BY k);" # { serverError 241 }
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 9830400;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 4096) AS k, uniqCombined(reinterpretAsString(number % 4096)) u FROM numbers(4096 * 100) GROUP BY k);"

$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT 'K=16';"

# HashTable for UInt32 (used until (1<<12) elements), hence 4096 elements
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT 'UInt32';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 2000000;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 4096) AS k, uniqCombined(16)(number % 4096) u FROM numbers(4096 * 100) GROUP BY k);" # { serverError 241 }
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 4915200;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 4096) AS k, uniqCombined(16)(number % 4096) u FROM numbers(4096 * 100) GROUP BY k);"

# HashTable for UInt64 (used until (1<<11) elements), hence 2048 elements
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT 'UInt64';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 2000000;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 2048) AS k, uniqCombined(16)(reinterpretAsString(number % 2048)) u FROM numbers(2048 * 100) GROUP BY k);" # { serverError 241 }
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 4915200;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 2048) AS k, uniqCombined(16)(reinterpretAsString(number % 2048)) u FROM numbers(2048 * 100) GROUP BY k);"

$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT 'K=18';"

# HashTable for UInt32 (used until (1<<14) elements), hence 16384 elements
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT 'UInt32';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 8000000;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 16384) AS k, uniqCombined(18)(number % 16384) u FROM numbers(16384 * 100) GROUP BY k);" # { serverError 241 }
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 19660800;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 16384) AS k, uniqCombined(18)(number % 16384) u FROM numbers(16384 * 100) GROUP BY k);"

# HashTable for UInt64 (used until (1<<13) elements), hence 8192 elements
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT 'UInt64';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 8000000;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 8192) AS k, uniqCombined(18)(reinterpretAsString(number % 8192)) u FROM numbers(8192 * 100) GROUP BY k);" # { serverError 241 }
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = 19660800;"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT sum(u) FROM (SELECT intDiv(number, 8192) AS k, uniqCombined(18)(reinterpretAsString(number % 8192)) u FROM numbers(8192 * 100) GROUP BY k);"
