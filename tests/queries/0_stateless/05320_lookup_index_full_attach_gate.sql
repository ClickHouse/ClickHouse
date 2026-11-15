-- A user-supplied full `ATTACH` introduces new `LOOKUP INDEX` metadata just like `CREATE`,
-- so it must be gated by `allow_experimental_lookup_index` as well.

DROP TABLE IF EXISTS lookup_attach_full;

SET allow_experimental_lookup_index = 0;

ATTACH TABLE lookup_attach_full (id UInt64, LOOKUP INDEX idx_set (id) TYPE table_set) ENGINE = MergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'lookup_attach_full';
