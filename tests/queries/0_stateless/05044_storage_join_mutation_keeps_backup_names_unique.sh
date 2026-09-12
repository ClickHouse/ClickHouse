#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- uses a server-wide failpoint that would pause the persistent `Set`/`Join`
# inserts of concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation consolidates the committed backups into a single file, while an insert that reserved
# its backup number before the mutation started may still be streaming its staged file and will
# promote it afterwards. The mutation must not rewind the backup-name counter below such in-flight
# names: doing so let a later insert reserve the same name and overwrite the in-flight insert's
# committed backup, losing its rows on restart.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_mutation_names;
    CREATE TABLE join_mutation_names (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO join_mutation_names VALUES (1, 'one');
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT set_or_join_sink_pause_before_publish"

# The in-flight insert: it has staged its backup under its reserved name and pauses before
# publishing it, holding no locks.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_mutation_names VALUES (2, 'two')" &
inflight_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT set_or_join_sink_pause_before_publish PAUSE"

# The mutation deletes nothing, but rewrites the committed backups into a consolidated file.
$CLICKHOUSE_CLIENT --query "ALTER TABLE join_mutation_names DELETE WHERE k = 0 SETTINGS mutations_sync = 2"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT set_or_join_sink_pause_before_publish"

wait $inflight_pid

# A later insert must get a fresh backup name instead of reusing the in-flight insert's one.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_mutation_names VALUES (3, 'three')"

$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_names ORDER BY k"

# The persisted backups must match the live state: reattaching rebuilds the state from disk.
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_mutation_names;
    ATTACH TABLE join_mutation_names;
"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_mutation_names ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_mutation_names"
