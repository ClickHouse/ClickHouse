#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: relies on a failpoint (libfiu), which the fast-test build does not include.
# A dependency registered concurrently (after the pre-shutdown check passes) makes
# DETACH DICTIONARY ... PERMANENTLY throw HAVE_DEPENDENT_OBJECTS after the dictionary was already
# shut down and deregistered from the loader. The rejected detach must leave the dictionary usable.
# A failpoint pauses the detach between shutdown and dependency removal so the dependent can be
# created deterministically in that window.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
CREATE TABLE src_race (id UInt64, val String) ENGINE = Memory;
INSERT INTO src_race VALUES (1, 'a');
CREATE DICTIONARY d_race (id UInt64, val String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src_race' DB currentDatabase()))
LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);
SELECT dictGetString('d_race', 'val', 1);
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT detach_permanently_pause_before_remove_dependencies"

# The detach passes the pre-shutdown check (no dependent yet), shuts the dictionary down, then pauses.
$CLICKHOUSE_CLIENT --query "DETACH DICTIONARY d_race PERMANENTLY" > /dev/null 2>&1 &
detach_pid=$!

# Wait until the detach is actually paused after shutdown, then register a dependent in that window.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT detach_permanently_pause_before_remove_dependencies PAUSE"
$CLICKHOUSE_CLIENT --query "CREATE TABLE dep_race (id UInt64, v String DEFAULT dictGetString('d_race', 'val', id)) ENGINE = Memory"

# Release the detach: dependency removal now throws HAVE_DEPENDENT_OBJECTS after shutdown.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT detach_permanently_pause_before_remove_dependencies"
wait "$detach_pid"

# Regression: the dictionary must still be usable after the rejected detach.
$CLICKHOUSE_CLIENT --query "SELECT dictGetString('d_race', 'val', 1)"

$CLICKHOUSE_CLIENT --query "
DROP TABLE dep_race;
DROP DICTIONARY d_race;
DROP TABLE src_race;
"
