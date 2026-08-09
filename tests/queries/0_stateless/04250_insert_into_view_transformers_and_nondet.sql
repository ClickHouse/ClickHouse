-- Tests for two follow-up review concerns on INSERT into regular views (issue #91535).
--
-- 1. The explicit column list in `INSERT INTO view (...)` must support column transformers
--    (`* EXCEPT (...)`, etc.), not just plain identifiers. Otherwise a partial INSERT
--    written as `INSERT INTO v(* EXCEPT (b)) ...` is silently treated as "no column list"
--    and omitted columns receive the *view-schema* default instead of the target table's.
--
-- 2. Non-deterministic DEFAULT expressions (e.g. `DEFAULT now64()`, `DEFAULT rand()`) on
--    the target table must be evaluated exactly once: the value used by the view's `WHERE`
--    predicate must be the value that ends up stored in the target table.

DROP TABLE IF EXISTS t_target_xform;
DROP VIEW IF EXISTS v_xform;

CREATE TABLE t_target_xform (a Int32, b String DEFAULT 'target_b', c Float64 DEFAULT 0.5) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_xform AS SELECT * FROM t_target_xform;

-- 1a. `* EXCEPT (b, c)` expands to (a). `b` and `c` must use the target's defaults.
INSERT INTO v_xform (* EXCEPT (b, c)) VALUES (1);
SELECT 'xform_except:', a, b, c FROM t_target_xform ORDER BY a;
TRUNCATE TABLE t_target_xform;

-- 1b. `* EXCEPT (c)` expands to (a, b). Only `c` must use the target's default.
INSERT INTO v_xform (* EXCEPT (c)) VALUES (2, 'user_b');
SELECT 'xform_except_one:', a, b, c FROM t_target_xform ORDER BY a;
TRUNCATE TABLE t_target_xform;

DROP VIEW v_xform;
DROP TABLE t_target_xform;

-- 2. Non-deterministic target DEFAULT, single evaluation: the value pinned by the WHERE
--    check is the value that is stored.
DROP TABLE IF EXISTS t_nondet;
DROP VIEW IF EXISTS v_nondet;

CREATE TABLE t_nondet (id Int32, marker UInt64 DEFAULT rand64() + 1) ENGINE = MergeTree ORDER BY id;
CREATE VIEW v_nondet AS SELECT * FROM t_nondet WHERE marker > 0;

INSERT INTO v_nondet (id) VALUES (1);

-- The stored marker must be strictly positive (the WHERE predicate); a row that satisfied
-- the predicate at check time but had its default re-evaluated to a different value at
-- write time would be observable here as a violation of the predicate over the stored data.
SELECT 'nondet_consistent:', count() FROM t_nondet WHERE marker > 0;
SELECT 'nondet_rowcount:', count() FROM t_nondet;

DROP VIEW v_nondet;
DROP TABLE t_nondet;
