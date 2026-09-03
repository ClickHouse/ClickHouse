CREATE TABLE replacing_wrong (key Int64, ver Int64, is_deleted UInt16) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE replacing_wrong (key Int64, ver String, is_deleted UInt8) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE replacing_wrong (key Int64, ver Int64, is_deleted UInt8) ENGINE = ReplacingMergeTree(is_deleted, is_deleted) ORDER BY key; -- { serverError BAD_ARGUMENTS }

CREATE TABLE replacing (key Int64, ver Int64, is_deleted UInt8) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key;
ALTER TABLE replacing MODIFY COLUMN ver String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE replacing MODIFY COLUMN ver Int128;
ALTER TABLE replacing MODIFY COLUMN is_deleted String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE replacing MODIFY COLUMN is_deleted UInt16; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE replacing MODIFY COLUMN is_deleted Int8; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE replacing DROP COLUMN ver; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE replacing DROP COLUMN is_deleted; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE replacing RENAME COLUMN ver TO ver2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE replacing RENAME COLUMN is_deleted TO is_deleted2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

CREATE TABLE collapsing_wrong (key Int64, sign Int16) ENGINE = CollapsingMergeTree(sign) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE collapsing_wrong (key Int64, sign UInt8) ENGINE = CollapsingMergeTree(sign) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE collapsing_wrong (key Int64, sign UInt8) ENGINE = CollapsingMergeTree(not_existing) ORDER BY key; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

CREATE TABLE collapsing (key Int64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY key;
ALTER TABLE collapsing MODIFY COLUMN sign String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE collapsing DROP COLUMN sign; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE collapsing RENAME COLUMN sign TO sign2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE collapsing MODIFY COLUMN sign MODIFY SETTING max_compress_block_size = 123456; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

CREATE TABLE versioned_collapsing_wrong (key Int64, version UInt8, sign Int8) ENGINE = VersionedCollapsingMergeTree(sign, sign) ORDER BY key; -- { serverError BAD_ARGUMENTS }

CREATE TABLE versioned_collapsing (key Int64, version UInt8, sign Int8) ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY key;
