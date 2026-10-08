#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS 03775_join, 03775_data"

# Check with both temporary and regular tables
# Explicitly set settings:
# - enable_analyzer = 1 -- old analyzer rewrites table identifiers to use current database before resolving them in joinGet
# - enable_parallel_replicas = 0 -- temporary table won't be available for secondary parallel replicas queries
$CLICKHOUSE_CLIENT -nm -q "
    CREATE TABLE 03775_data (key UInt32) ENGINE = MergeTree ORDER BY tuple() AS SELECT number FROM numbers(5);

    CREATE TEMPORARY TABLE 03775_join (key UInt32, val String) ENGINE = Join(ANY, LEFT, key) AS SELECT 1, 'foo' UNION ALL SELECT 3, 'bar';
    CREATE TABLE 03775_join (key UInt32, val String) ENGINE = Join(ANY, LEFT, key) AS SELECT 1, 'baz' UNION ALL SELECT 3, 'qux';

    SELECT '-- Assert joinGet table resolved to temporary';
    SELECT joinGet('03775_join', 'val', 1::UInt32);
    SELECT joinGet('03775_join', 'val', 2::UInt32);
    SELECT joinGet(03775_join, 'val', 3::UInt32) SETTINGS enable_analyzer = 1;
    SELECT joinGet(03775_join, 'val', 4::UInt32) SETTINGS enable_analyzer = 1;

    SELECT '----';
    SELECT key, joinGet('03775_join', 'val', key) FROM 03775_data ORDER BY 1 SETTINGS enable_parallel_replicas = 0;

    SELECT '-- Assert joinGet table resolved to regular when explicitly specified';
    SELECT joinGet('$CLICKHOUSE_DATABASE.03775_join', 'val', 1::UInt32);
    SELECT joinGet('$CLICKHOUSE_DATABASE.03775_join', 'val', 2::UInt32);
    SELECT joinGet($CLICKHOUSE_DATABASE.03775_join, 'val', 3::UInt32);
    SELECT joinGet($CLICKHOUSE_DATABASE.03775_join, 'val', 4::UInt32);

    SELECT '----';
    SELECT key, joinGet('$CLICKHOUSE_DATABASE.03775_join', 'val', key) FROM 03775_data ORDER BY 1;
"

# Check with only regular table
# Explicitly set settings:
# - enable_parallel_replicas = 0 -- secondary queries may use different "current database", so table identifier
#   without database specified won't be resolved properly
$CLICKHOUSE_CLIENT -nm -q "
    SELECT '-- Assert joinGet table resolved to regular';
    SELECT joinGet('03775_join', 'val', 1::UInt32);
    SELECT joinGet('03775_join', 'val', 2::UInt32);
    SELECT joinGet(03775_join, 'val', 3::UInt32);
    SELECT joinGet(03775_join, 'val', 4::UInt32);

    SELECT '----';
    SELECT key, joinGet('03775_join', 'val', key) FROM 03775_data ORDER BY 1 SETTINGS enable_parallel_replicas = 0;
"

# Negative testcases: non-existing table and the table with non-join engine
$CLICKHOUSE_CLIENT -q "SELECT joinGet('03775_fake_table', 'val', 123) -- { serverError UNKNOWN_TABLE }"
$CLICKHOUSE_CLIENT -nm -q "
    CREATE TEMPORARY TABLE 03775_mem (key UInt32, val String) ENGINE = Memory AS SELECT 1, 'foo';

    SELECT joinGet('03775_mem', 'val', 1) -- { serverError ILLEGAL_TYPE_OF_ARGUMENT };
"

$CLICKHOUSE_CLIENT -q "DROP TABLE 03775_join, 03775_data;"
