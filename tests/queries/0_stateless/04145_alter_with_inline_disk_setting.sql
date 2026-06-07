-- Tags: no-fasttest, no-parallel

-- Regression for https://github.com/ClickHouse/ClickHouse/issues/63019
--
-- ALTER on a table created with `SETTINGS disk = disk(...)` (inline custom-disk function)
-- previously failed with `Bad get: has CustomType, requested String. (BAD_GET)` because the
-- ALTER pipeline did not convert the `CustomType` `Field` (the parsed `disk(...)` AST) to a
-- registered disk name `String` before passing it to `BaseSettings::applyChanges` /
-- `MergeTreeSettings::checkCanSet` / `safeGet<String>`. The CREATE path
-- (`MergeTreeSettingsImpl::loadFromQuery`) already did this conversion, so CREATE worked but
-- ANY subsequent ALTER (even one that does not touch `disk`) failed because every ALTER
-- re-applies the table's `settings_changes` AST through the validation pipeline.

DROP TABLE IF EXISTS t_63019;

-- The user-provided `path` deliberately differs in its last component from the disk `name`.
-- The metadata storage default at `<context.getPath()>/disks/<disk_name>/` would otherwise
-- collide with the object storage path on builds where the server's working directory
-- coincides with its data path (CI's run_r0 layout), causing intermittent
-- `create_directory: No such file or directory` failures on the metadata-side `store/`
-- subdirectory. Mirrors the path style of `02963_test_flexible_disk_configuration.sql`.
CREATE TABLE t_63019 (a Int32, b Int64) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '63019_disk_default',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './63019_disk_default_objstore/');

INSERT INTO t_63019 SELECT number, number * 10 FROM numbers(10);
SELECT count(), sum(a) FROM t_63019;
SELECT a, b FROM t_63019 ORDER BY a;

-- ADD COLUMN previously failed with `BAD_GET` because `MergeTreeData::checkAlterIsPossible`
-- and `MergeTreeData::checkColumnFilenamesForCollision` re-apply `settings_changes` to
-- validate the new metadata, hitting `SettingFieldString::operator=` for the still-`CustomType`
-- `disk` value.
ALTER TABLE t_63019 ADD COLUMN c String DEFAULT 'x' AFTER b;
SELECT count(), sum(a), uniq(c) FROM t_63019;
SELECT a, b, c FROM t_63019 ORDER BY a;

-- MODIFY COLUMN takes the same path and used to fail too.
ALTER TABLE t_63019 MODIFY COLUMN b UInt64;
SELECT count(), sum(a) FROM t_63019;
SELECT a, b, c FROM t_63019 ORDER BY a;

-- MODIFY SETTING for a non-`disk` setting also re-applies the table's `settings_changes`
-- through `MergeTreeData::changeSettings`, which used to fail with `BAD_GET` even though the
-- `disk` value is not being modified by this ALTER.
ALTER TABLE t_63019 MODIFY SETTING merge_with_ttl_timeout = 60;
SELECT count(), sum(a) FROM t_63019;
SELECT a, b, c FROM t_63019 ORDER BY a;

-- INSERT after ALTERs — exercises the actual disk write path and verifies the table is still
-- functional, not just the metadata transitions.
INSERT INTO t_63019 SELECT number + 100, number * 100, 'y' FROM numbers(5);
SELECT count(), sum(a) FROM t_63019;
SELECT a, b, c FROM t_63019 ORDER BY a;

DROP TABLE t_63019;

-- @PedroTadim's variant from issue #63019: `ALTER TABLE ... MODIFY SETTING disk = disk(...)`,
-- where the new value is itself an inline `disk(...)` function. The CustomType `Field` must be
-- converted to a registered disk-name `String` before `MergeTreeData::changeSettings` calls
-- `change.value.safeGet<String>()` on the incoming change. Without `convertCustomDiskSettings`
-- on this path, every `MODIFY SETTING disk = disk(...)` threw `BAD_GET` (code 170) — even
-- before any cache validation or storage-policy migration check could run.

DROP TABLE IF EXISTS t_63019_modify_disk;

-- See note above on disk `name` vs `path`-last-component: the path here uses
-- `63019_modify_disk_objstore/` (different from `name = '63019_modify_disk'`) to avoid the
-- metadata-vs-object-storage directory collision in CI flaky-check iterations.
CREATE TABLE t_63019_modify_disk (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '63019_modify_disk',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './63019_modify_disk_objstore/');

INSERT INTO t_63019_modify_disk SELECT number FROM numbers(5);
SELECT count(), sum(a) FROM t_63019_modify_disk;
SELECT a FROM t_63019_modify_disk ORDER BY a;

-- Re-applying the SAME inline disk via `MODIFY SETTING disk = disk(...)` is a no-op for the
-- storage policy (same disk → same registered policy), but it still goes through
-- `convertCustomDiskSettings` and `change.value.safeGet<String>()` — the exact code path that
-- threw `BAD_GET` before the fix.
ALTER TABLE t_63019_modify_disk MODIFY SETTING disk = disk(
    name = '63019_modify_disk',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './63019_modify_disk_objstore/');
SELECT count(), sum(a) FROM t_63019_modify_disk;
SELECT a FROM t_63019_modify_disk ORDER BY a;

-- @PedroTadim's exact variant: `MODIFY SETTING disk = disk(type = cache, ...)` wrapping the
-- existing inline disk in a cache layer. Before the fix this threw `BAD_GET` (code 170) at the
-- `safeGet<String>` call. After the fix the `CustomType` conversion succeeds and the request
-- is rejected by the storage-policy migration guard with `BAD_ARGUMENTS` (code 36) — changing
-- a table's `disk` to a different storage policy is not supported in general because data on
-- the original disk would become unreachable. The crucial observation here is that we no
-- longer see `BAD_GET`; the operation reaches a domain-level validation, not a `Field` type
-- crash.
ALTER TABLE t_63019_modify_disk MODIFY SETTING disk = disk(
    name = '63019_modify_disk_cache',
    type = cache,
    disk = '63019_modify_disk',
    path = './63019_modify_disk_cache_data/',
    max_size = '1Mi'); -- { serverError BAD_ARGUMENTS }

-- The rejected ALTER above must not corrupt or alter the table's data — verify both the
-- aggregate and the row-level read path still work after the rollback.
SELECT count(), sum(a) FROM t_63019_modify_disk;
SELECT a FROM t_63019_modify_disk ORDER BY a;

DROP TABLE t_63019_modify_disk;

-- clickhouse-gh[bot] review on PR #103818: the same `BAD_GET` is reachable through a fourth
-- call site that the original three-site fix missed — the `UNIQUE KEY` storage-policy
-- validation guard in `MergeTreeData::checkAlterIsPossible`. That guard iterates
-- `new_metadata.settings_changes->as<const ASTSetQuery &>().changes` and calls
-- `safeGet<String>` on every `disk`/`storage_policy` value before any of the other three
-- conversion sites are reached. For a `UNIQUE KEY` table created with inline
-- `SETTINGS disk = disk(...)`, that field is still a parser `CustomType` in metadata, so any
-- later `ALTER` that lands in this guard previously threw `Bad get: has CustomType,
-- requested String` independently of the original fix. The companion fix in this commit
-- normalizes a mutable copy via `DiskFromAST::convertCustomDiskSettings` before the guard
-- loop runs; this test exercises that path.

DROP TABLE IF EXISTS t_63019_uk;

SET allow_experimental_unique_key = 1;

-- A local custom disk under the test environment's `custom_local_disks_base_directory`
-- (`/var/lib/clickhouse/disks/`, configured by `tests/config/config.d/custom_disks_base_path.xml`).
-- `UNIQUE KEY` rejects any disk whose `DataSourceDescription::type != Local`, so the
-- `object_storage` / `local_blob_storage` style used by the other test cases above
-- (which is classified as `ObjectStorage`) cannot be used here.
CREATE TABLE t_63019_uk (id UInt64, v String) ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS disk = disk(
    name = '63019_uk_local',
    type = local,
    path = '/var/lib/clickhouse/disks/04145_uk_local/');

INSERT INTO t_63019_uk SELECT number, toString(number) FROM numbers(5);
SELECT count(), sum(id) FROM t_63019_uk;
SELECT id, v FROM t_63019_uk ORDER BY id;

-- ADD COLUMN — runs `MergeTreeData::checkAlterIsPossible`, which (after applying the
-- alter to `new_metadata`) walks `new_metadata.settings_changes` inside the `UNIQUE KEY`
-- storage-policy guard. Without the new `convertCustomDiskSettings` call this hit
-- `safeGet<String>` on the `CustomType` `disk` value and threw `BAD_GET`.
ALTER TABLE t_63019_uk ADD COLUMN extra String DEFAULT '-' AFTER v;
SELECT count(), sum(id), uniq(extra) FROM t_63019_uk;
SELECT id, v, extra FROM t_63019_uk ORDER BY id;

-- MODIFY SETTING for a non-`disk` setting — still re-applies the table's
-- `settings_changes` AST through the guard, still tripped `BAD_GET` even though the
-- ALTER itself does not touch `disk`.
ALTER TABLE t_63019_uk MODIFY SETTING merge_with_ttl_timeout = 60;
SELECT count(), sum(id) FROM t_63019_uk;
SELECT id, v, extra FROM t_63019_uk ORDER BY id;

-- INSERT after ALTERs — verifies the read/write path on the inline custom local disk
-- continues to work end-to-end, not just the metadata transitions.
INSERT INTO t_63019_uk SELECT number + 100, toString(number + 100), 'y' FROM numbers(3);
SELECT count(), sum(id) FROM t_63019_uk;
SELECT id, v, extra FROM t_63019_uk ORDER BY id;

DROP TABLE t_63019_uk;

-- clickhouse-gh[bot] review on PR #103818: `DiskFromAST::convertCustomDiskSettings` is not just a
-- normalisation step; for an inline `disk(...)` it calls `createCustomDisk`, which mutates the
-- global disk selector via `Context::getOrCreateDisk`. Several validation paths
-- (`AlterCommand::apply`, `MergeTreeData::checkAlterIsPossible`, the `UNIQUE KEY` guard,
-- `checkColumnFilenamesForCollision`) run on metadata copies and can throw AFTER the disk has
-- been registered (e.g. the storage-policy migration guard rejects the new disk with
-- `BAD_ARGUMENTS`). Without the rollback scope, the rejected `MODIFY SETTING disk = disk(...)`
-- ALTER would leave the inline disk's name reserved in the global selector until restart, and a
-- later valid `CREATE TABLE` reusing the same name with different settings would fail as if
-- another table already owned it.
--
-- This block exercises the rollback: a `MODIFY SETTING disk = disk(name = '63019_rollback_v1',
-- ...)` ALTER rejected by the storage-policy migration guard must NOT leak `63019_rollback_v1`,
-- so a subsequent `CREATE TABLE ... SETTINGS disk = disk(name = '63019_rollback_v1', ...)` with
-- different settings succeeds rather than throwing
-- "The disk `63019_rollback_v1` is already configured as a custom disk in another table".

DROP TABLE IF EXISTS t_63019_rollback_a;
DROP TABLE IF EXISTS t_63019_rollback_b;

CREATE TABLE t_63019_rollback_a (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '63019_rollback_a',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './63019_rollback_a_objstore/');

INSERT INTO t_63019_rollback_a SELECT number FROM numbers(3);
SELECT count(), sum(a) FROM t_63019_rollback_a;

-- Try to wrap in a cache layer named `63019_rollback_v1`. The storage-policy migration guard
-- rejects this with `BAD_ARGUMENTS` (cannot change `disk` to a different storage policy).
-- Before the rollback scope, `63019_rollback_v1` would be left in the global `DiskSelector` and
-- the next CREATE below would fail.
ALTER TABLE t_63019_rollback_a MODIFY SETTING disk = disk(
    name = '63019_rollback_v1',
    type = cache,
    disk = '63019_rollback_a',
    path = './63019_rollback_v1_data/',
    max_size = '1Mi'); -- { serverError BAD_ARGUMENTS }

-- The rejected ALTER must not corrupt or alter the table's data.
SELECT count(), sum(a) FROM t_63019_rollback_a;

-- Now register `63019_rollback_v1` as a fresh table with DIFFERENT settings (different inner
-- disk path and a different max_size). With the rollback scope this succeeds; without it the
-- prior leaked registration of `63019_rollback_v1` (with `disk = '63019_rollback_a'` and
-- `path = './63019_rollback_v1_data/'`) collides on settings hash and throws
-- "The disk `63019_rollback_v1` is already configured as a custom disk in another table".
CREATE TABLE t_63019_rollback_b (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '63019_rollback_v1',
    type = cache,
    disk = '63019_rollback_a',
    path = './63019_rollback_v1_other_data/',
    max_size = '2Mi');

INSERT INTO t_63019_rollback_b SELECT number FROM numbers(2);
SELECT count(), sum(a) FROM t_63019_rollback_b;

DROP TABLE t_63019_rollback_b;
DROP TABLE t_63019_rollback_a;
