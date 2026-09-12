-- Invalid definitions of VersionedCoalescingMergeTree tables.

DROP TABLE IF EXISTS t_vcmt_errors;

-- The version argument is mandatory.
CREATE TABLE t_vcmt_errors (key UInt64, version UInt64, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree() ORDER BY key; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE t_vcmt_errors (key UInt64, version UInt64, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree ORDER BY key; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The version column name must be an identifier.
CREATE TABLE t_vcmt_errors (key UInt64, version UInt64, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree(42) ORDER BY key; -- { serverError BAD_ARGUMENTS }

-- The version column must exist.
CREATE TABLE t_vcmt_errors (key UInt64, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- The version column must be of an integer type or of type Date/DateTime/DateTime64.
CREATE TABLE t_vcmt_errors (key UInt64, version String, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE t_vcmt_errors (key UInt64, version Nullable(UInt64), a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }

-- The version column cannot be listed in the columns to coalesce.
CREATE TABLE t_vcmt_errors (key UInt64, version UInt64, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree(version, (version, a)) ORDER BY key; -- { serverError BAD_ARGUMENTS }

-- Columns to coalesce cannot be a part of the sorting key.
CREATE TABLE t_vcmt_errors (key UInt64, version UInt64, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree(version, (key)) ORDER BY key; -- { serverError BAD_ARGUMENTS }

-- The version column cannot be a part of the sorting key.
CREATE TABLE t_vcmt_errors (key UInt64, version UInt64, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree(version) ORDER BY (key, version); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_vcmt_errors (key UInt64, version UInt64, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree(version) ORDER BY (key, intHash32(version)); -- { serverError BAD_ARGUMENTS }

-- The version column cannot be a part of the partition key.
CREATE TABLE t_vcmt_errors (key UInt64, version UInt64, a Nullable(UInt64)) ENGINE = VersionedCoalescingMergeTree(version) PARTITION BY intDiv(version, 100) ORDER BY key; -- { serverError BAD_ARGUMENTS }

SELECT 'OK';
