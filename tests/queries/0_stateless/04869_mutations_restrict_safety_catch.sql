-- Tags: no-replicated-database
-- Exercise the `mutations_restrict` session setting: tier 1 rejects mutation-producing ALTER TABLE
-- forms; tier 2 additionally rejects standalone lightweight DELETE / UPDATE. Tier 0 preserves the
-- prior behavior. The user can lower the setting in-session; admins pin it with `<readonly/>`
-- (validated by the integration test test_mutations_restrict_readonly).

DROP TABLE IF EXISTS t_mutations_restrict;

CREATE TABLE t_mutations_restrict
(
    id UInt64,
    v  UInt64,
    s  String,
    a  Nullable(UInt32) DEFAULT NULL,
    INDEX v_idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_mutations_restrict SELECT number, number, toString(number), number FROM numbers(10);

-- Tier 0 (default): mutation-producing ALTER and lightweight DML both succeed.
SET mutations_restrict = 0;
ALTER TABLE t_mutations_restrict UPDATE v = v + 1 WHERE id = 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_mutations_restrict DELETE                 WHERE id = 1 SETTINGS mutations_sync = 2;
DELETE FROM t_mutations_restrict WHERE id = 2;
UPDATE t_mutations_restrict SET v = v + 1 WHERE id = 3 SETTINGS enable_lightweight_update = 1;

-- Tier 1: mutation-producing ALTER forms rejected, metadata-only ALTER and lightweight DML still succeed.
SET mutations_restrict = 1;

-- Rejected mutation-producing ALTERs (one per shape).
ALTER TABLE t_mutations_restrict UPDATE v = v + 1 WHERE id = 4;                     -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE t_mutations_restrict DELETE                 WHERE id = 5;                -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE t_mutations_restrict MATERIALIZE INDEX v_idx;                            -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE t_mutations_restrict MATERIALIZE COLUMN a;                               -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE t_mutations_restrict APPLY DELETED MASK;                                 -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE t_mutations_restrict DROP INDEX v_idx;                                   -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE t_mutations_restrict RENAME COLUMN s TO s2;                              -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE t_mutations_restrict DROP COLUMN s;                                      -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE t_mutations_restrict MODIFY COLUMN s Enum8('a' = 1);                     -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE t_mutations_restrict CLEAR COLUMN a IN PARTITION tuple();                -- { serverError QUERY_IS_PROHIBITED }

-- Metadata-only ALTERs are still allowed at tier 1.
ALTER TABLE t_mutations_restrict ADD COLUMN new_col UInt8 DEFAULT 0;
ALTER TABLE t_mutations_restrict COMMENT COLUMN v 'meta only';
ALTER TABLE t_mutations_restrict MODIFY COLUMN v UInt64 COMMENT 'still meta only';
ALTER TABLE t_mutations_restrict MODIFY COLUMN v Int64;                              -- widening: metadata-only conversion
ALTER TABLE t_mutations_restrict ADD CONSTRAINT c CHECK id >= 0;
ALTER TABLE t_mutations_restrict DROP CONSTRAINT c;

-- Lightweight DML still allowed at tier 1.
DELETE FROM t_mutations_restrict WHERE id = 6;
UPDATE t_mutations_restrict SET v = v + 1 WHERE id = 7 SETTINGS enable_lightweight_update = 1;

-- In-session override: lower to 0 and confirm ALTER UPDATE runs again.
SET mutations_restrict = 0;
ALTER TABLE t_mutations_restrict UPDATE v = v + 1 WHERE id = 8 SETTINGS mutations_sync = 2;

-- Tier 2: additionally reject lightweight DELETE / UPDATE.
SET mutations_restrict = 2;
ALTER TABLE t_mutations_restrict UPDATE v = v + 1 WHERE id = 0;                      -- { serverError QUERY_IS_PROHIBITED }
DELETE FROM t_mutations_restrict WHERE id = 0;                                       -- { serverError QUERY_IS_PROHIBITED }
UPDATE t_mutations_restrict SET v = 0 WHERE id = 0 SETTINGS enable_lightweight_update = 1; -- { serverError QUERY_IS_PROHIBITED }
-- Metadata-only ALTER, SELECT and INSERT still succeed.
ALTER TABLE t_mutations_restrict ADD COLUMN new_col2 UInt8 DEFAULT 0;
INSERT INTO t_mutations_restrict(id, v, s) VALUES (100, 100, '100');
SELECT count() FROM t_mutations_restrict WHERE id = 100;

DROP TABLE t_mutations_restrict;
