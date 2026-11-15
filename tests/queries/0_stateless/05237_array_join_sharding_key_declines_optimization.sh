#!/usr/bin/env bash
# An `arrayJoin` in a `Distributed` sharding key is not a per-row function of the source row: a single
# row can map to several shards, so the `optimize_distributed_group_by_sharding_key` shortcut, which
# assumes a shard's rows reach their final values on that shard alone, has to be declined for such a
# key. The planner meets one whenever a definition is replayed rather than stated - a short
# `ATTACH TABLE`, the tables of an `ATTACH DATABASE`, a `Replicated` database's DDL replay, a `RESTORE`,
# server startup - because a replayed definition loads whatever was stored, and declining the shortcut
# is then the only answer left.
#
# `clickhouse-local` over a prepared data directory is how the stored metadata is obtained here: the
# table is created with an ordinary sharding key, its stored definition is then edited to contain the
# `arrayJoin`, and the next start loads it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

# `clickhouse-local` has no clusters of its own, so the one the engine refers to is supplied here. It
# needs more than one shard: with a single shard the shortcut is not considered at all.
cat > "${WORKING_DIR}/config.xml" <<'EOF'
<clickhouse>
    <remote_servers>
        <two_shards>
            <shard><replica><host>127.0.0.1</host><port>9000</port></replica></shard>
            <shard><replica><host>127.0.0.2</host><port>9000</port></replica></shard>
        </two_shards>
    </remote_servers>
</clickhouse>
EOF

LOCAL="${CLICKHOUSE_LOCAL} --path ${WORKING_DIR} --config-file ${WORKING_DIR}/config.xml"
SETTINGS="SET enable_analyzer = 1, optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1, explain_query_plan_default = 'legacy';"

$LOCAL -q "
CREATE DATABASE db;
CREATE TABLE db.dst (k1 UInt32, k2 UInt32) ENGINE = MergeTree ORDER BY k1;
CREATE TABLE db.d (k1 UInt32, k2 UInt32) ENGINE = Distributed('two_shards', 'db', 'dst', cityHash64(k1, k2));
"

# Whether the initiator keeps a step of its own is the decision: `0` means the shortcut was taken and
# the shards' output needs nothing above the remote read, `1` means the initiator still merges or
# re-applies the clause, so the shortcut was declined. An ordinary key takes it, which is what the
# three lines below are read against. The key is probed once per clause that can carry it - `DISTINCT`,
# `GROUP BY` and `LIMIT BY` - and each probe is a separate entry into the walk, so all three are read.
$LOCAL -q "${SETTINGS}
SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT k1, k2 FROM db.d) WHERE explain ILIKE '%Distinct (DISTINCT)%';
SELECT count() > 0 FROM (EXPLAIN SELECT k1, k2 FROM db.d GROUP BY k1, k2) WHERE explain ILIKE '%MergingAggregated%';
SELECT count() > 0 FROM (EXPLAIN SELECT k1, k2 FROM db.d LIMIT 1 BY k1, k2) WHERE explain ILIKE '%LimitBy%';
"

sed -i 's/cityHash64(k1, k2))$/cityHash64(k1, arrayJoin([1, 2]), k2))/' "${WORKING_DIR}/metadata/db/d.sql"
grep -c -F 'arrayJoin([1, 2])' "${WORKING_DIR}/metadata/db/d.sql"

# The stored definition loads, otherwise the whole database would not.
$LOCAL -q "SELECT count() FROM system.tables WHERE database = 'db' AND name = 'd'"

# ... and the shortcut is declined instead of the walk over the key reaching an unreachable branch.
$LOCAL -q "${SETTINGS}
SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT k1, k2 FROM db.d) WHERE explain ILIKE '%Distinct (DISTINCT)%';
SELECT count() > 0 FROM (EXPLAIN SELECT k1, k2 FROM db.d GROUP BY k1, k2) WHERE explain ILIKE '%MergingAggregated%';
SELECT count() > 0 FROM (EXPLAIN SELECT k1, k2 FROM db.d LIMIT 1 BY k1, k2) WHERE explain ILIKE '%LimitBy%';
"

rm -rf "${WORKING_DIR}"
