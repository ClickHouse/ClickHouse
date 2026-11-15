#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ORDINARY_DB="${CLICKHOUSE_DATABASE}_ordinary"
NC_NAME="test_nc_dep_${CLICKHOUSE_DATABASE}"

# Setup: clean up any leftover state
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${NC_NAME};
"

# Create named collection and table that uses it
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${NC_NAME} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE test_nc_dep_table (x UInt32) ENGINE = URL(${NC_NAME});
"

# Should fail because table uses the named collection (check_named_collection_dependencies is true by default)
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }"

# With check_named_collection_dependencies disabled, drop should succeed even with dependent table
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION ${NC_NAME};
DROP TABLE test_nc_dep_table;
"

# Test normal behavior again with the setting enabled
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${NC_NAME} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE test_nc_dep_table (x UInt32) ENGINE = URL(${NC_NAME});
"

# Should fail again with setting enabled
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }"

# Test rename: dependency should be tracked after rename (Atomic database uses UUID, so rename is transparent)
$CLICKHOUSE_CLIENT -q "RENAME TABLE test_nc_dep_table TO test_nc_dep_table_renamed;"
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }"

# Test EXCHANGE TABLES in Atomic database (UUID-based tracking handles this automatically)
NC_NAME2="test_nc_dep2_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${NC_NAME2};
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${NC_NAME2} AS url = 'http://localhost:8123', format = 'JSON';
CREATE TABLE test_nc_dep_table2 (x UInt32) ENGINE = URL(${NC_NAME2});
"

# Both named collections should be protected
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }"
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME2}; -- { serverError NAMED_COLLECTION_IS_USED }"

# Exchange the tables - dependencies should still work (UUID-based tracking)
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES test_nc_dep_table_renamed AND test_nc_dep_table2;"

# After exchange, both named collections should still be protected
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }"
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME2}; -- { serverError NAMED_COLLECTION_IS_USED }"

# Clean up
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE test_nc_dep_table_renamed;
DROP TABLE test_nc_dep_table2;
DROP NAMED COLLECTION ${NC_NAME};
DROP NAMED COLLECTION ${NC_NAME2};
"

# Test with mixed databases: one table in Atomic, one in Ordinary, both using the same named collection
# This tests both UUID-based and name-based tracking simultaneously
# Use --send_logs_level=error to suppress the deprecation warning for Ordinary database
$CLICKHOUSE_CLIENT --send_logs_level=error -m -q "
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS ${ORDINARY_DB};
CREATE DATABASE ${ORDINARY_DB} ENGINE = Ordinary;
"

$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${NC_NAME} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE test_nc_dep_atomic (x UInt32) ENGINE = URL(${NC_NAME});
CREATE TABLE ${ORDINARY_DB}.test_nc_dep_ordinary (x UInt32) ENGINE = URL(${NC_NAME});
"

# Should fail because both tables use the named collection
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }"

# Test rename in Ordinary database (name-based tracking should update)
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${ORDINARY_DB}.test_nc_dep_ordinary TO ${ORDINARY_DB}.test_nc_dep_ordinary_renamed;"
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }"

# Drop the Atomic table, should still fail because Ordinary table uses the collection
$CLICKHOUSE_CLIENT -q "DROP TABLE test_nc_dep_atomic;"
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }"

# Drop the Ordinary table, now drop should succeed
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE ${ORDINARY_DB}.test_nc_dep_ordinary_renamed;
DROP NAMED COLLECTION ${NC_NAME};
DROP DATABASE ${ORDINARY_DB};
"
