CREATE TABLE t_cuckoo_bad (`k` UInt64, INDEX i k TYPE cuckoo_filter(0.05) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64; -- { serverError SUPPORT_IS_DISABLED }
SET allow_experimental_cuckoo_filter_index = 1;
CREATE TABLE t_cuckoo_bad (`k` UInt64, INDEX i k TYPE cuckoo_filter(0.) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_cuckoo_bad (`k` UInt64, INDEX i k TYPE cuckoo_filter(1.) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_cuckoo_bad (`k` UInt64, INDEX i k TYPE cuckoo_filter(1.5) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_cuckoo_bad (`k` UInt64, INDEX i k TYPE cuckoo_filter(-0.01) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_cuckoo_bad (`k` UInt64, INDEX i k TYPE cuckoo_filter(nan) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_cuckoo_bad (`k` UInt64, INDEX i k TYPE cuckoo_filter(0.01, 0.02) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- One ALTER with DROP INDEX + ADD INDEX (same name): must not bypass experimental gate when FPR/expression/granularity change.
DROP TABLE IF EXISTS t_cuckoo_alter_bypass_fpr;
DROP TABLE IF EXISTS t_cuckoo_alter_bypass_expr;
DROP TABLE IF EXISTS t_cuckoo_alter_bypass_gran;

-- Existing experimental index: unrelated ALTER and DROP succeed with setting off.
DROP TABLE IF EXISTS t_cuckoo_alter_keep;
SET allow_experimental_cuckoo_filter_index = 1;
CREATE TABLE t_cuckoo_alter_keep (`k` UInt64, INDEX idx k TYPE cuckoo_filter(0.01) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
SET allow_experimental_cuckoo_filter_index = 0;
ALTER TABLE t_cuckoo_alter_keep MODIFY COLUMN k COMMENT 'note';
SELECT 'cuckoo_alter_keep_unrelated_ok';
SELECT 1;
ALTER TABLE t_cuckoo_alter_keep DROP INDEX idx;
SELECT 'cuckoo_alter_drop_ok';
SELECT 1;
DROP TABLE IF EXISTS t_cuckoo_alter_keep;

DROP TABLE IF EXISTS t_cuckoo_alter_add;
SET allow_experimental_cuckoo_filter_index = 1;
CREATE TABLE t_cuckoo_alter_add (`k` UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
SET allow_experimental_cuckoo_filter_index = 0;
ALTER TABLE t_cuckoo_alter_add ADD INDEX idx k TYPE cuckoo_filter(0.01) GRANULARITY 1; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE IF EXISTS t_cuckoo_alter_add;

SET allow_experimental_cuckoo_filter_index = 1;
CREATE TABLE t_cuckoo_alter_bypass_fpr (`k` UInt64, INDEX idx k TYPE cuckoo_filter(0.01) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE t_cuckoo_alter_bypass_expr (`k` UInt64, INDEX idx k TYPE cuckoo_filter(0.01) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE t_cuckoo_alter_bypass_gran (`k` UInt64, INDEX idx k TYPE cuckoo_filter(0.01) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
SET allow_experimental_cuckoo_filter_index = 0;
ALTER TABLE t_cuckoo_alter_bypass_fpr
    DROP INDEX idx,
    ADD INDEX idx k TYPE cuckoo_filter(0.02) GRANULARITY 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_cuckoo_alter_bypass_expr
    DROP INDEX idx,
    ADD INDEX idx (k + 1) TYPE cuckoo_filter(0.01) GRANULARITY 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_cuckoo_alter_bypass_gran
    DROP INDEX idx,
    ADD INDEX idx k TYPE cuckoo_filter(0.01) GRANULARITY 2; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE IF EXISTS t_cuckoo_alter_bypass_fpr;
DROP TABLE IF EXISTS t_cuckoo_alter_bypass_expr;
DROP TABLE IF EXISTS t_cuckoo_alter_bypass_gran;
