#!/usr/bin/env bash
# The pre-shutdown dependency check must stay exactly as strict as before: a dependent that already
# exists still makes DROP / DETACH ... PERMANENTLY fail, before anything is shut down, so the object
# is left fully usable. RENAME, whose only dependency check happens while removing the edges, must
# keep throwing too. No failpoint here -- this is the "must not act" side of the change and it also
# passes on a server without the fix.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT --query "
CREATE TABLE src (id UInt64, val String) ENGINE = Memory;
INSERT INTO src VALUES (1, 'a');
CREATE DICTIONARY dict (id UInt64, val String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB '${DB}'))
LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);
CREATE TABLE dep (id UInt64, v String DEFAULT dictGetString('${DB}.dict', 'val', id)) ENGINE = Memory;
"

echo "-- dictionary: DETACH PERMANENTLY with a pre-existing dependent is still rejected"
$CLICKHOUSE_CLIENT --query "DETACH DICTIONARY dict PERMANENTLY" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | head -1
echo "-- and the dictionary is untouched"
$CLICKHOUSE_CLIENT --query "SELECT dictGetString('${DB}.dict', 'val', 1)"

echo "-- dictionary: DROP with a pre-existing dependent is still rejected"
$CLICKHOUSE_CLIENT --query "DROP DICTIONARY dict" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | head -1
$CLICKHOUSE_CLIENT --query "SELECT dictGetString('${DB}.dict', 'val', 1)"

echo "-- dictionary: RENAME with a dependent is still rejected"
$CLICKHOUSE_CLIENT --query "RENAME DICTIONARY dict TO dict2" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | head -1
$CLICKHOUSE_CLIENT --query "SELECT dictGetString('${DB}.dict', 'val', 1)"

echo "-- MergeTree: rejected, and the table stays readable and writable"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE mt (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO mt VALUES (1, 'm');
CREATE DICTIONARY dep_mt (id UInt64, val String) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'mt' DB '${DB}')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);
"
$CLICKHOUSE_CLIENT --query "DETACH TABLE mt PERMANENTLY" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | head -1
$CLICKHOUSE_CLIENT --query "INSERT INTO mt VALUES (2, 'n')"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM mt"

echo "-- MaterializedView: rejected, and insert-through still works"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE mv_src (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id AS SELECT id, val FROM mv_src;
CREATE DICTIONARY dep_mv (id UInt64, val String) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'mv' DB '${DB}')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);
INSERT INTO mv_src VALUES (1, 'x');
"
$CLICKHOUSE_CLIENT --query "DETACH TABLE mv PERMANENTLY" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | head -1
$CLICKHOUSE_CLIENT --query "INSERT INTO mv_src VALUES (2, 'y')"
# The rejected detach must not have stopped insert-through: source and view stay in step.
$CLICKHOUSE_CLIENT --query "SELECT count() = (SELECT count() FROM mv_src) FROM mv"

echo "-- with both dependency checks disabled nothing is checked, as before"
$CLICKHOUSE_CLIENT --query "
SET check_referential_table_dependencies = 0, check_table_dependencies = 0;
DETACH DICTIONARY dict PERMANENTLY;
"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dict'"
# ... and nothing is logged either: with no check there is no blocking set to warn about.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --query "
SELECT count()
FROM system.text_log
WHERE level = 'Warning'
  AND message LIKE 'Removing ${DB}.dict %still depend on it%'
"

$CLICKHOUSE_CLIENT --query "
DROP DICTIONARY dep_mv;
DROP DICTIONARY dep_mt;
DROP TABLE mv;
DROP TABLE mv_src;
DROP TABLE mt;
DROP TABLE dep;
DROP TABLE src;
"
