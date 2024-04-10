CREATE TABLE replacing_wrong (key Int64, ver Int64, is_deleted UInt16) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE replacing_wrong (key Int64, ver String, is_deleted UInt8) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }

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

CREATE TABLE summing_wrong (key Int64, sum1 Int64, sum2 String) ENGINE = SummingMergeTree((sum1, sum2)) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE summing_wrong (key Int64, sum1 String, sum2 Int64) ENGINE = SummingMergeTree((sum1, sum2)) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE summing_wrong (key Int64, sum1 String, sum2 Int64) ENGINE = SummingMergeTree(sum_doesnt_exists) ORDER BY key; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

CREATE TABLE summing (key Int64, sum1 Int64, sum2 UInt64, not_sum String) ENGINE = SummingMergeTree((sum1, sum2)) ORDER BY key;
ALTER TABLE summing MODIFY COLUMN sum1 Int32, MODIFY COLUMN sum2 IPv4; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE summing MODIFY COLUMN sum2 String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE summing MODIFY COLUMN sum1 Int32, MODIFY COLUMN sum2 UInt256;
ALTER TABLE summing DROP COLUMN sum1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE summing DROP COLUMN sum2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE summing RENAME COLUMN sum1 TO sum3; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE summing RENAME COLUMN sum2 TO sum3; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE summing RENAME COLUMN not_sum TO still_not_sum;

CREATE TABLE collapsing_wrong (key Int64, sign Int16) ENGINE = CollapsingMergeTree(sign) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE collapsing_wrong (key Int64, sign UInt8) ENGINE = CollapsingMergeTree(sign) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE collapsing_wrong (key Int64, sign UInt8) ENGINE = CollapsingMergeTree(not_existing) ORDER BY key; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

CREATE TABLE collapsing (key Int64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY key;
ALTER TABLE collapsing MODIFY COLUMN sign String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE collapsing DROP COLUMN sign; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE collapsing RENAME COLUMN sign TO sign2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
