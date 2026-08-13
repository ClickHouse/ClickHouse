-- `CLEAR COLUMN` validation must see the same dependencies as the mutation executor, which expands
-- `ALIAS` bodies and canonicalizes subcolumn reads before deciding whether the recalculation of a
-- MATERIALIZED column is legal. A dependency hidden behind an ALIAS or a subcolumn must be rejected
-- up front, in `ALTER` validation, not after the mutation has been queued.

-- A MATERIALIZED column reads an EPHEMERAL column through an ALIAS.
DROP TABLE IF EXISTS t_clear_alias_dep;

CREATE TABLE t_clear_alias_dep
(
    a UInt8,
    e UInt8 EPHEMERAL a + 100,
    x UInt8 ALIAS e,
    m UInt8 MATERIALIZED a + x
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_clear_alias_dep (a) VALUES (1);

ALTER TABLE t_clear_alias_dep CLEAR COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The rejection happened in validation: nothing was queued.
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_clear_alias_dep';

DROP TABLE t_clear_alias_dep;

-- A MATERIALIZED column in the sorting key reads the cleared column through a subcolumn.
CREATE TABLE t_clear_subcolumn_dep
(
    t Tuple(a UInt8, b UInt8),
    m UInt8 MATERIALIZED t.a + 1
) ENGINE = MergeTree ORDER BY m;

INSERT INTO t_clear_subcolumn_dep (t) VALUES ((1, 2));

ALTER TABLE t_clear_subcolumn_dep CLEAR COLUMN t; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_clear_subcolumn_dep';

DROP TABLE t_clear_subcolumn_dep;

-- A subcolumn read that does not lead to a key or an EPHEMERAL column is still recalculated.
CREATE TABLE t_clear_subcolumn_ok
(
    t Tuple(a UInt8, b UInt8),
    k UInt8,
    m UInt8 MATERIALIZED t.a + 1
) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_clear_subcolumn_ok (t, k) VALUES ((1, 2), 1);

ALTER TABLE t_clear_subcolumn_ok CLEAR COLUMN t SETTINGS mutations_sync = 1;

SELECT t, k, m FROM t_clear_subcolumn_ok;

DROP TABLE t_clear_subcolumn_ok;
