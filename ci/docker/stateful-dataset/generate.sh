#!/bin/bash
# Bakes the hits_v1/visits_v1 stateful datasets into a local, self-contained
# (table_disk) store at /opt/ch-stateful, so clickhouse/stateless-test (which is
# built FROM clickhouse/clickhouse-stateful-test) can attach them instantly from a local
# disk instead of lazily reading them from the web disk (real AWS S3), which is
# too slow. See tests/docker_scripts/create.sql for the serve-time (read-only)
# attach and ci/jobs/scripts/clickhouse_proc.py for the symlink that exposes
# /opt/ch-stateful under custom_local_disks_base_directory.
set -euo pipefail

DEST=/opt/ch-stateful
GEN="$(mktemp -d)"
cd "$GEN"

# The image has no clickhouse binary; fetch a static one just to run this one-off
# generation. The on-disk MergeTree format is backward compatible, so the server
# under test (built from the PR) reads what a recent stable produced here.
curl -fsSL https://clickhouse.com/ | sh

# Read each dataset from its web-disk source and copy it into a local
# plain_rewritable table_disk store under $DEST. plain_rewritable is writable
# during generation; create.sql serves the very same store with readonly = true.
# `CREATE ... AS <src>` copies the column list, so the schema lives only in
# create_source.sql (kept in sync with tests/docker_scripts/create.sql).
./clickhouse local --path "$GEN/state" --multiquery "
CREATE DATABASE datasets;

$(cat /opt/gen/create_source.sql)

-- The part layout of the store must be deterministic and fully merged (one
-- part per partition): several stateful tests assert the part layout of
-- test.hits (00098_primary_key_memory_allocated, 00166_explain_estimate,
-- 00183_prewhere_conditions_order, ...), matching the fully merged layout of
-- the previous web-disk snapshot, while a parallel INSERT leaves a
-- nondeterministic number of parts (whatever background merges got to).
-- Insert and OPTIMIZE ... FINAL in a scratch table on the local disk (deleted
-- with \$GEN), then clone only the merged parts into the store: OPTIMIZE
-- directly on the store table would leave the merged-away source parts
-- behind, because outdated parts are not removed from disk on shutdown.
CREATE DATABASE scratch;
CREATE DATABASE staging;

CREATE TABLE scratch.hits_v1 AS datasets.hits_v1
ENGINE = MergeTree
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
INSERT INTO scratch.hits_v1 SELECT * FROM datasets.hits_v1 SETTINGS max_insert_threads = 16;
OPTIMIZE TABLE scratch.hits_v1 FINAL;

CREATE TABLE staging.hits_v1 AS datasets.hits_v1
ENGINE = MergeTree
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS table_disk = 1,
    disk = disk(type = object_storage, object_storage_type = local_blob_storage, metadata_type = plain_rewritable, path = '$DEST/hits_v1/');
ALTER TABLE staging.hits_v1 ATTACH PARTITION ALL FROM scratch.hits_v1;

CREATE TABLE scratch.visits_v1 AS datasets.visits_v1
ENGINE = CollapsingMergeTree(Sign)
PARTITION BY toYYYYMM(StartDate)
ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
SAMPLE BY intHash32(UserID);
INSERT INTO scratch.visits_v1 SELECT * FROM datasets.visits_v1 SETTINGS max_insert_threads = 16;
OPTIMIZE TABLE scratch.visits_v1 FINAL;

CREATE TABLE staging.visits_v1 AS datasets.visits_v1
ENGINE = CollapsingMergeTree(Sign)
PARTITION BY toYYYYMM(StartDate)
ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
SAMPLE BY intHash32(UserID)
SETTINGS table_disk = 1,
    disk = disk(type = object_storage, object_storage_type = local_blob_storage, metadata_type = plain_rewritable, path = '$DEST/visits_v1/');
ALTER TABLE staging.visits_v1 ATTACH PARTITION ALL FROM scratch.visits_v1;

SELECT 'Store part layout:', database, table, name, rows FROM system.parts WHERE database = 'staging' AND active FORMAT TSV;
"

echo "Baked stateful dataset sizes:"
du -sh "$DEST"/*

cd /
rm -rf "$GEN"
