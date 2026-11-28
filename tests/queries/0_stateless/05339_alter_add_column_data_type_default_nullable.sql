DROP TABLE IF EXISTS t_alter_default_nullable;
CREATE TABLE t_alter_default_nullable (i Int32) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_alter_default_nullable VALUES (1), (2);

SET data_type_default_nullable = 1;
ALTER TABLE t_alter_default_nullable ADD COLUMN k Int32;
ALTER TABLE t_alter_default_nullable ADD COLUMN k_null Int32 NULL;
ALTER TABLE t_alter_default_nullable ADD COLUMN k_not_null Int32 NOT NULL;
ALTER TABLE t_alter_default_nullable ADD COLUMN k_nullable Nullable(Int32);
ALTER TABLE t_alter_default_nullable ADD COLUMN k_default Int32 DEFAULT 7;
ALTER TABLE t_alter_default_nullable ADD COLUMN k_default_null Int32 DEFAULT NULL;
ALTER TABLE t_alter_default_nullable ADD COLUMN s String, ADD COLUMN u UInt8;
ALTER TABLE t_alter_default_nullable ADD COLUMN a Array(Int32); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
ALTER TABLE t_alter_default_nullable ADD COLUMN lc LowCardinality(String); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
ALTER TABLE t_alter_default_nullable ADD COLUMN a Array(Int32) NOT NULL;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_default_nullable' ORDER BY position;
SELECT * FROM t_alter_default_nullable ORDER BY i;

SET data_type_default_nullable = 0;
ALTER TABLE t_alter_default_nullable ADD COLUMN k_off Int32, ADD COLUMN k_off_not_null Int32;
INSERT INTO t_alter_default_nullable (i, k) VALUES (3, 30);

SET data_type_default_nullable = 1;
ALTER TABLE t_alter_default_nullable MODIFY COLUMN k Int64;
ALTER TABLE t_alter_default_nullable MODIFY COLUMN k_off Int64;
ALTER TABLE t_alter_default_nullable MODIFY COLUMN k_off_not_null Int64 NOT NULL;
ALTER TABLE t_alter_default_nullable MODIFY COLUMN k_off_not_null COMMENT 'c';
ALTER TABLE t_alter_default_nullable MODIFY COLUMN a Array(Int64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_default_nullable' AND name IN ('k', 'a', 'k_off', 'k_off_not_null') ORDER BY position;
SELECT i, k FROM t_alter_default_nullable ORDER BY i;
DROP TABLE t_alter_default_nullable;

SET data_type_default_nullable = 0;
DROP TABLE IF EXISTS t_modify_state_version;
CREATE TABLE t_modify_state_version (i Int32, t Tuple(s AggregateFunction(quantileDeterministic, UInt64, UInt64)), u Tuple(s AggregateFunction(quantileDeterministic, UInt64, UInt64))) ENGINE = MergeTree ORDER BY i;
ALTER TABLE t_modify_state_version MODIFY COLUMN u Nullable(Tuple(s AggregateFunction(quantileDeterministic, UInt64, UInt64)));
-- With the setting, MODIFY COLUMN gives the same type as the explicit Nullable: the state version is not pinned.
SET data_type_default_nullable = 1;
ALTER TABLE t_modify_state_version MODIFY COLUMN t Tuple(s AggregateFunction(quantileDeterministic, UInt64, UInt64));
DETACH TABLE t_modify_state_version;
ATTACH TABLE t_modify_state_version;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_modify_state_version' ORDER BY position;
DROP TABLE t_modify_state_version;
