#!/usr/bin/env bash
# Test that dictionaries using named collections are tracked as dependencies,
# preventing DROP NAMED COLLECTION while the dictionary exists.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NC_NAME="test_nc_dict_dep_${CLICKHOUSE_DATABASE}"

# Setup: clean up any leftover state
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP DICTIONARY IF EXISTS test_nc_dict_dep;
DROP NAMED COLLECTION IF EXISTS ${NC_NAME};
"

# Create named collection and dictionary that uses it (via ClickHouse source)
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE test_nc_dict_dep_source (id UInt64, value String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_nc_dict_dep_source VALUES (1, 'one'), (2, 'two');

CREATE NAMED COLLECTION ${NC_NAME} AS
    host = 'localhost',
    port = ${CLICKHOUSE_PORT_TCP},
    user = 'default',
    password = '',
    db = '${CLICKHOUSE_DATABASE}',
    table = 'test_nc_dict_dep_source';

CREATE DICTIONARY test_nc_dict_dep
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(NAME ${NC_NAME}))
LAYOUT(FLAT())
LIFETIME(0);
"

# Verify dictionary works
$CLICKHOUSE_CLIENT -q "SELECT dictGet('${CLICKHOUSE_DATABASE}.test_nc_dict_dep', 'value', toUInt64(1));"

# Should fail: dictionary uses the named collection
$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }"

# With check disabled, drop should succeed
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION ${NC_NAME};
"

# Clean up
$CLICKHOUSE_CLIENT -m -q "
DROP DICTIONARY IF EXISTS test_nc_dict_dep;
DROP TABLE IF EXISTS test_nc_dict_dep_source;
"
