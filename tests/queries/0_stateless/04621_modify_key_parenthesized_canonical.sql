-- MODIFY ORDER BY / MODIFY SAMPLE BY must store the canonical (unparenthesized) key form,
-- the same as the storage-level clauses at CREATE time, so SHOW CREATE and the on-disk
-- metadata do not keep the redundant parentheses until the next reparse.

DROP TABLE IF EXISTS t_modify_order_by_parens;
CREATE TABLE t_modify_order_by_parens (x UInt64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_modify_order_by_parens ADD COLUMN a UInt64, MODIFY ORDER BY (a);
SHOW CREATE TABLE t_modify_order_by_parens;
DROP TABLE t_modify_order_by_parens;

DROP TABLE IF EXISTS t_modify_order_by_list_parens;
CREATE TABLE t_modify_order_by_list_parens (a UInt64) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_modify_order_by_list_parens ADD COLUMN b UInt64, MODIFY ORDER BY ((a), (b));
SHOW CREATE TABLE t_modify_order_by_list_parens;
DROP TABLE t_modify_order_by_list_parens;

DROP TABLE IF EXISTS t_modify_sample_by_parens;
CREATE TABLE t_modify_sample_by_parens (a UInt64) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_modify_sample_by_parens MODIFY SAMPLE BY (a);
SHOW CREATE TABLE t_modify_sample_by_parens;
DROP TABLE t_modify_sample_by_parens;
