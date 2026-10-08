#!/usr/bin/env bash
# A part detached before a metadata-only `ALTER` and then missing its `metadata_version.txt` - a file
# the part checksums do not cover - used to be attached at the table's *current* version, so the
# pending `RENAME COLUMN` was skipped and every row of the renamed column read as its default. Such a
# part must be refused instead. `clickhouse local` is used because the test removes a file from a part.
# Every case is set up in one run and checked in the next one, because each start of `clickhouse local`
# is slow in sanitizer builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05171_${CLICKHOUSE_DATABASE}"
rm -rf "${workdir}"
mkdir -p "${workdir}"

reloaded_path=$(${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
-- A plain rename.
CREATE TABLE renamed (id UInt64, a UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO renamed SELECT number, number FROM numbers(1000);
ALTER TABLE renamed DETACH PARTITION tuple();
ALTER TABLE renamed RENAME COLUMN a TO b;

-- A rename into a name freed by a drop leaves the set of names looking plausible: the part holds
-- a, b, the table holds a, and only the table's own record of the rename tells that the part's b
-- is the table's current a. Reading the part at the current version would serve the stale a.
CREATE TABLE reused (id UInt64, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO reused SELECT number, 0, number FROM numbers(1000);
ALTER TABLE reused DETACH PARTITION tuple();
ALTER TABLE reused DROP COLUMN a;
ALTER TABLE reused RENAME COLUMN b TO a;

-- A swap of two columns leaves the set of names identical (one ALTER refuses transitive renames,
-- so the swap takes three).
CREATE TABLE swapped (id UInt64, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO swapped SELECT number, 0, number FROM numbers(1000);
ALTER TABLE swapped DETACH PARTITION tuple();
ALTER TABLE swapped RENAME COLUMN a TO tmp;
ALTER TABLE swapped RENAME COLUMN b TO a;
ALTER TABLE swapped RENAME COLUMN tmp TO b;

-- A type changed meanwhile: the part holds the column under its old type, which its own columns.txt
-- records, and MergeTree picks the conversion by the data version, not by the missing file, so the part
-- is converted as usual. (ReplicatedMergeTree picks it by the metadata version and refuses such a part.)
CREATE TABLE retyped (id UInt64, value UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO retyped SELECT number, number FROM numbers(1000);
ALTER TABLE retyped DETACH PARTITION tuple();
ALTER TABLE retyped MODIFY COLUMN value String;

-- The same missing file over an unchanged schema: the part's columns match the table's, so reading it
-- at the current version is the same as reading it at its own.
CREATE TABLE unchanged (id UInt64, a UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO unchanged SELECT number, number FROM numbers(1000);
ALTER TABLE unchanged DETACH PARTITION tuple();

-- And with a column dropped meanwhile: the part carries a column the table does not, but the table has
-- nothing the part lacks, so the part is still readable as it is.
CREATE TABLE dropped (id UInt64, a UInt32, c UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO dropped SELECT number, number, number FROM numbers(1000);
ALTER TABLE dropped DETACH PARTITION tuple();
ALTER TABLE dropped DROP COLUMN c;

-- A column dropped and another added meanwhile: the part holds a column the table does not and lacks
-- one the table has, as after a rename, but the table remembers the drop, which explains the extra
-- column; the added column reads as its default either way.
CREATE TABLE dropped_added (id UInt64, a UInt32, c UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO dropped_added SELECT number, number, number FROM numbers(1000);
ALTER TABLE dropped_added DETACH PARTITION tuple();
ALTER TABLE dropped_added DROP COLUMN c;
ALTER TABLE dropped_added ADD COLUMN b UInt32 DEFAULT 7;

-- A column dropped and then added again under the same name: the names match, but the part's value
-- is the stale data from before the drop, while the re-added column must read as its default. The
-- table's record of the drop tells them apart.
CREATE TABLE readded (id UInt64, value UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO readded SELECT number, number FROM numbers(1000);
ALTER TABLE readded DETACH PARTITION tuple();
ALTER TABLE readded DROP COLUMN value;
ALTER TABLE readded ADD COLUMN value UInt32 DEFAULT 7;

-- A column cleared meanwhile: CLEAR COLUMN keeps the column in the table, so it is not a drop followed
-- by an add of the same name and the part still attaches.
CREATE TABLE cleared (id UInt64, a UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO cleared SELECT number, number FROM numbers(1000);
ALTER TABLE cleared DETACH PARTITION tuple();
ALTER TABLE cleared CLEAR COLUMN a IN PARTITION tuple();

-- A part with a persistent virtual column (_row_exists, written by a lightweight delete) and a column
-- added meanwhile: the virtual column is not among the table's columns, but it is not a sign of a rename.
CREATE TABLE with_virtual (id UInt64, a UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO with_virtual SELECT number, number FROM numbers(1000);
DELETE FROM with_virtual WHERE id >= 500;
ALTER TABLE with_virtual DETACH PARTITION tuple();
ALTER TABLE with_virtual ADD COLUMN b UInt32 DEFAULT 7;

-- The reused-name rename again, but with the part found in the table's directory when the table is
-- loaded, not attached: the table reads its mutations before its parts, so the part is refused there
-- too (and detached as broken) instead of serving the stale a.
CREATE TABLE reloaded (id UInt64, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO reloaded SELECT number, 0, number FROM numbers(1000);
ALTER TABLE reloaded DETACH PARTITION tuple();
ALTER TABLE reloaded DROP COLUMN a;
ALTER TABLE reloaded RENAME COLUMN b TO a;

SELECT arrayJoin(data_paths) FROM system.tables WHERE database = currentDatabase() AND name = 'reloaded';
")

find "${workdir}/store" -path '*/detached/*/metadata_version.txt' -delete
mv "${reloaded_path}detached/all_1_1_0" "${reloaded_path}"

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
ALTER TABLE renamed ATTACH PARTITION tuple(); -- { serverError CORRUPTED_DATA }
SELECT 'renamed refused', (SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'renamed') AS still_detached, (SELECT count() FROM renamed) AS attached;

ALTER TABLE reused ATTACH PARTITION tuple(); -- { serverError CORRUPTED_DATA }
SELECT 'reused refused', (SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'reused') AS still_detached, (SELECT count() FROM reused) AS attached;

ALTER TABLE swapped ATTACH PARTITION tuple(); -- { serverError CORRUPTED_DATA }
SELECT 'swapped refused', (SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'swapped') AS still_detached, (SELECT count() FROM swapped) AS attached;

ALTER TABLE readded ATTACH PARTITION tuple(); -- { serverError CORRUPTED_DATA }
SELECT 'readded refused', (SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'readded') AS still_detached, (SELECT count() FROM readded) AS attached;

ALTER TABLE unchanged ATTACH PARTITION tuple();
SELECT 'unchanged schema attaches', count(), sum(a) FROM unchanged;

ALTER TABLE dropped ATTACH PARTITION tuple();
SELECT 'dropped column attaches', count(), sum(a) FROM dropped;

ALTER TABLE dropped_added ATTACH PARTITION tuple();
SELECT 'dropped and added columns attach', count(), sum(a), sum(b) FROM dropped_added;

ALTER TABLE retyped ATTACH PARTITION tuple();
SELECT 'retyped column attaches', count(), sum(toUInt64(value)), toTypeName(any(value)) FROM retyped;

ALTER TABLE cleared ATTACH PARTITION tuple();
SELECT 'cleared column attaches', count(), sum(a) FROM cleared;

ALTER TABLE with_virtual ATTACH PARTITION tuple();
SELECT 'persistent virtual column attaches', count(), sum(a), sum(b) FROM with_virtual;

SELECT 'reloaded part not served', count() FROM reloaded;
"

# A broken part is moved to `detached` in the background after the load: look at it from a later run.
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'reloaded part detached', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'reloaded' AND startsWith(reason, 'broken');
"

rm -rf "${workdir}"
