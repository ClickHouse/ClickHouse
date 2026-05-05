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

CREATE TABLE t_63019 (a Int32, b Int64) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '63019_disk_default',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './disks/63019_default/');

INSERT INTO t_63019 SELECT number, number * 10 FROM numbers(10);
SELECT count(), sum(a) FROM t_63019;

-- ADD COLUMN previously failed with `BAD_GET` because `MergeTreeData::checkAlterIsPossible`
-- and `MergeTreeData::checkColumnFilenamesForCollision` re-apply `settings_changes` to
-- validate the new metadata, hitting `SettingFieldString::operator=` for the still-`CustomType`
-- `disk` value.
ALTER TABLE t_63019 ADD COLUMN c String DEFAULT 'x' AFTER b;
SELECT count(), sum(a), uniq(c) FROM t_63019;

-- MODIFY COLUMN takes the same path and used to fail too.
ALTER TABLE t_63019 MODIFY COLUMN b UInt64;
SELECT count(), sum(a) FROM t_63019;

-- MODIFY SETTING for a non-`disk` setting also re-applies the table's `settings_changes`
-- through `MergeTreeData::changeSettings`, which used to fail with `BAD_GET` even though the
-- `disk` value is not being modified by this ALTER.
ALTER TABLE t_63019 MODIFY SETTING merge_with_ttl_timeout = 60;
SELECT count(), sum(a) FROM t_63019;

-- INSERT after ALTERs — exercises the actual disk write path and verifies the table is still
-- functional, not just the metadata transitions.
INSERT INTO t_63019 SELECT number + 100, number * 100, 'y' FROM numbers(5);
SELECT count(), sum(a) FROM t_63019;

DROP TABLE t_63019;

-- @PedroTadim's variant from issue #63019: `ALTER TABLE ... MODIFY SETTING disk = disk(...)`,
-- where the new value is itself an inline `disk(...)` function. The CustomType `Field` must be
-- converted to a registered disk-name `String` before `MergeTreeData::changeSettings` calls
-- `change.value.safeGet<String>()` on the incoming change. Without `convertCustomDiskSettings`
-- on this path, every `MODIFY SETTING disk = disk(...)` threw `BAD_GET` (code 170) — even
-- before any cache validation or storage-policy migration check could run.

DROP TABLE IF EXISTS t_63019_modify_disk;

CREATE TABLE t_63019_modify_disk (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '63019_modify_disk',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './disks/63019_modify_disk/');

INSERT INTO t_63019_modify_disk SELECT number FROM numbers(5);

-- Re-applying the SAME inline disk via `MODIFY SETTING disk = disk(...)` is a no-op for the
-- storage policy (same disk → same registered policy), but it still goes through
-- `convertCustomDiskSettings` and `change.value.safeGet<String>()` — the exact code path that
-- threw `BAD_GET` before the fix.
ALTER TABLE t_63019_modify_disk MODIFY SETTING disk = disk(
    name = '63019_modify_disk',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './disks/63019_modify_disk/');
SELECT count(), sum(a) FROM t_63019_modify_disk;

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
    path = './filesystem_caches/63019_modify_disk_cache/',
    max_size = '1Mi'); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_63019_modify_disk;
