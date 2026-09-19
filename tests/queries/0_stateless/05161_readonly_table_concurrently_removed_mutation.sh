#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-object-storage: the test defines its own plain_rewritable disk
# Tag no-replicated-database: `DETACH TABLE` is rejected there, and plain rewritable should not be
# shared between replicas
# Tag no-shared-merge-tree: does not support replication

# `table_disk` puts the data directory at the disk root, so a readonly table over the same
# plain_rewritable path shares that directory with the live table that writes it. The readonly table
# caches the disk namespace in memory (see 04318_system_restart_disk_plain_rewritable), so it keeps
# listing a `mutation_*.txt` that the writer has already deleted, and reading it fails its ATTACH.
# A removed entry may only be ignored when no active part of the readonly table is still below its
# version; otherwise that entry is what supplies the part's on-the-fly alter conversions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_args="type = object_storage, object_storage_type = local, metadata_type = plain_rewritable,
           enable_hard_links = 1, path = 'disks/05161/${CLICKHOUSE_DATABASE}/'"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE writer (key Int32, value UInt32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = true, disk = disk(name = '05161_writer_${CLICKHOUSE_DATABASE}', ${disk_args}),
         min_bytes_for_wide_part = 0, old_parts_lifetime = 600;

INSERT INTO writer SELECT number, number FROM numbers(100);
ALTER TABLE writer UPDATE value = value + 1 WHERE 1 SETTINGS mutations_sync = 1;
ALTER TABLE writer UPDATE value = value + 10 WHERE 1 SETTINGS mutations_sync = 1;

CREATE TABLE reader (key Int32, value UInt32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = true,
         disk = disk(readonly = true, name = '05161_reader_${CLICKHOUSE_DATABASE}', ${disk_args});
"

echo '-- a finished mutation entry removed by the writer is ignored by the readonly table'
finished=$(${CLICKHOUSE_CLIENT} --query "
    SELECT min(mutation_id) FROM system.mutations
    WHERE database = currentDatabase() AND table = 'writer' AND is_done")
[ -n "${finished}" ] || echo "unexpected: no finished mutation was found to kill"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE reader"
${CLICKHOUSE_CLIENT} --query "
    KILL MUTATION WHERE database = currentDatabase() AND table = 'writer' AND mutation_id = '${finished}'" > /dev/null
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE reader"
${CLICKHOUSE_CLIENT} -m --query "
SELECT count(), sum(value) FROM reader;
SELECT count() FROM (SELECT * FROM writer EXCEPT SELECT * FROM reader);
SELECT mutation_id = '${finished}' FROM system.mutations WHERE database = currentDatabase() AND table = 'reader';
"

echo '-- an entry above the readonly parts is still needed by them, so it must not be ignored'
${CLICKHOUSE_CLIENT} -m --query "
SYSTEM STOP MERGES writer;
ALTER TABLE writer UPDATE value = value + 100 WHERE 1 SETTINGS mutations_sync = 0;

CREATE TABLE reader_unapplied (key Int32, value UInt32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = true,
         disk = disk(readonly = true, name = '05161_reader2_${CLICKHOUSE_DATABASE}', ${disk_args});
"
unapplied=$(${CLICKHOUSE_CLIENT} --query "
    SELECT mutation_id FROM system.mutations
    WHERE database = currentDatabase() AND table = 'writer' AND NOT is_done")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE reader_unapplied"
${CLICKHOUSE_CLIENT} --query "
    KILL MUTATION WHERE database = currentDatabase() AND table = 'writer' AND mutation_id = '${unapplied}'" > /dev/null
if error=$(${CLICKHOUSE_CLIENT} --query "ATTACH TABLE reader_unapplied" 2>&1); then
    echo "unexpected: ATTACH succeeded"
elif [ -z "${unapplied}" ]; then
    echo "unexpected: no unfinished mutation was found to kill"
elif echo "${error}" | grep -qF "${unapplied}"; then
    echo 'ATTACH failed and named the missing entry'
else
    echo "unexpected: ${error}"
fi

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE reader SYNC;
DROP TABLE writer SYNC;
"
