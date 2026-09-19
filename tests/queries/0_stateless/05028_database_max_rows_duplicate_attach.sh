#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree
# no-shared-merge-tree: relies on `ReplicatedMergeTree` skipping already-attached parts
# by their `_replace_from_` block ids during `ATTACH PARTITION FROM`.

# The database `max_rows` limit must be charged only for parts that ZooKeeper actually
# accepts: parts of an `ATTACH PARTITION FROM` that are deduplicated by their block ids
# are no-ops and must not count, even when the database is at or over the limit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DA="${CLICKHOUSE_DATABASE}_a"
CH="${CLICKHOUSE_CLIENT}"

$CH -q "DROP DATABASE IF EXISTS ${DA}"

$CH -q "
SELECT '-- attach from a source table records per-part block ids in ZooKeeper';
CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 100;
CREATE TABLE ${DA}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES ${DA}.src;
INSERT INTO ${DA}.src SELECT number FROM numbers(30);
CREATE TABLE ${DA}.r (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r1') ORDER BY x;
ALTER TABLE ${DA}.r ATTACH PARTITION ID 'all' FROM ${DA}.src;
SELECT count() FROM ${DA}.r;
SELECT '-- prepare a second source part and fill the database close to the limit';
INSERT INTO ${DA}.src SELECT number FROM numbers(5);
CREATE TABLE ${DA}.s (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/s', 'r1') ORDER BY x;
-- Two separate detached parts: the first one alone would still fit into the database, so a
-- per-part check would commit it before the second part throws.
SYSTEM STOP MERGES ${DA}.s;
INSERT INTO ${DA}.s SELECT number FROM numbers(10);
INSERT INTO ${DA}.s SELECT number + 10 FROM numbers(10);
ALTER TABLE ${DA}.s DETACH PARTITION ALL;
CREATE TABLE ${DA}.filler (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DA}.filler SELECT number FROM numbers(20);
SELECT rows FROM system.databases WHERE name = '${DA}';
SELECT '-- partially duplicated attach charges only the non-duplicate remainder, which fits';
ALTER TABLE ${DA}.r ATTACH PARTITION ID 'all' FROM ${DA}.src;
SELECT count() FROM ${DA}.r;
SELECT rows FROM system.databases WHERE name = '${DA}';
SELECT '-- fully duplicated attach is a no-op even at the limit';
ALTER TABLE ${DA}.r ATTACH PARTITION ID 'all' FROM ${DA}.src;
SELECT count() FROM ${DA}.r;
SELECT rows FROM system.databases WHERE name = '${DA}';
"

echo "-- a non-duplicate multi-part attach into the full database throws without attaching any part"
$CH -q "ALTER TABLE ${DA}.s ATTACH PARTITION ALL" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
# the failed attach must not have added rows, not even the first part that would have fit
$CH -q "SELECT count() FROM ${DA}.s; DROP DATABASE ${DA}"
