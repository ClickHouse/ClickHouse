-- https://github.com/ClickHouse/ClickHouse/issues/109214

-- A named transformer (APPLY with a prefix, or REPLACE) over a bare matcher/asterisk
-- expansion is rejected: there is no single column to carry the name.
SELECT * APPLY (x -> compound_value.*, 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * APPLY (x -> *, 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * APPLY (x -> COLUMNS('a'), 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (compound_value.* AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (* AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (COLUMNS('a') AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * APPLY (x -> *, 'f_') FROM (SELECT 1 AS a, 2 AS b); -- { serverError BAD_ARGUMENTS }
SELECT * APPLY (x -> COLUMNS('a'), 'f_') FROM (SELECT 1 AS a, 2 AS b); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (COLUMNS('a') AS a) FROM (SELECT 1 AS a, 2 AS b); -- { serverError BAD_ARGUMENTS }
-- A matcher nested inside a function is not a bare matcher, so it is still allowed.
SELECT * APPLY (x -> tuple(*), 'f_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
-- A named APPLY over `untuple` gives one prefixed column per tuple field (`f_a.1`, `f_a.id`).
SELECT * APPLY (x -> untuple(x), 'f_') FROM (SELECT (1, 2) AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> untuple(x), 'f_') FROM (SELECT cast((1, 2), 'Tuple(id UInt8, v UInt8)') AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> untuple(x), 'f_') FROM (SELECT (1, 2) AS a, (3, 4) AS b) FORMAT TSVWithNames;
SELECT * APPLY (untuple, 'f_') FROM (SELECT (1, 2) AS a) FORMAT TSVWithNames;
-- A single-field tuple keeps the field suffix (`f_a.id`, `f_a.1`) instead of collapsing to `f_a`.
SELECT * APPLY (untuple, 'f_') FROM (SELECT CAST(tuple(1), 'Tuple(id UInt8)') AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> untuple(x), 'f_') FROM (SELECT tuple(1) AS a) FORMAT TSVWithNames;
-- A named `REPLACE (untuple(a) AS a)` gives one column per tuple field named after the
-- REPLACE target (`a.1`, `a.id`).
SELECT * REPLACE (untuple(a) AS a) FROM (SELECT (1, 2) AS a) FORMAT TSVWithNames;
SELECT * REPLACE (untuple(a) AS a) FROM (SELECT cast((1, 2), 'Tuple(id UInt8, v UInt8)') AS a) FORMAT TSVWithNames;
SELECT * REPLACE (untuple(a) AS a) FROM (SELECT (1, 2) AS a, 5 AS c) FORMAT TSVWithNames;
SELECT * REPLACE (untuple(a) AS a) FROM (SELECT CAST(tuple(1), 'Tuple(id UInt8)') AS a) FORMAT TSVWithNames;
SELECT * REPLACE (untuple(a) AS a) FROM (SELECT tuple(1) AS a) FORMAT TSVWithNames;
-- The expanded names come from the REPLACE target, not the untupled source expression.
SELECT * REPLACE (untuple(b) AS a) FROM (SELECT 10 AS a, (1, 2) AS b) FORMAT TSVWithNames;
-- `untuple` must be terminal: a transformer chained after it is rejected.
SELECT * APPLY (x -> untuple(x), 'f_') APPLY toString FROM (SELECT (1, 2) AS a); -- { serverError UNSUPPORTED_METHOD }

SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'p_') APPLY (x -> x + 1, 'q_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * REPLACE (a + 1 AS a) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
-- The prefix uses the short column name (`f_a`), not the qualified projection name (`f_x.a`)
-- that a qualifying scope stores.
SELECT 99 AS a, x.* APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) AS x FORMAT TSVWithNames;
-- A chained transformer names its argument from the previous expression, not from the prefix
-- alias: `upper(toString(a))`, `toString(identity(a))`.
SELECT * APPLY (toString, 'f_') APPLY upper FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (identity, 'p_') APPLY toString FROM (SELECT 1 AS a) FORMAT TSVWithNames;
-- A named `untuple` chained after an earlier transformer prefixes the accumulated display name
-- feeding untuple (`q_identity(a).N`, `q_p_a.N`), not the original column name.
SELECT * APPLY identity APPLY (untuple, 'q_') FROM (SELECT (1, 2) AS a) FORMAT TSVWithNames;
SELECT * APPLY (identity, 'p_') APPLY (untuple, 'q_') FROM (SELECT (1, 2) AS a) FORMAT TSVWithNames;
-- An identity lambda (`x -> x`) resolves back to the matched node, so the accumulated prefix
-- is visible to a transformer chained after it (`toString(p_a)`).
SELECT * APPLY (x -> x, 'p_') APPLY toString FROM (SELECT 1 AS a) FORMAT TSVWithNames;

-- The prefix stays inside its own transformer chain. Matched column nodes are shared with
-- every other expression in the query, so a second matcher in the same SELECT reports plain
-- names regardless of order.
SELECT * APPLY (x -> x, 'p_'), * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT *, * APPLY (x -> x, 'p_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> x, 'p_'), * FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT *, * APPLY (x -> x, 'p_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
-- A qualified matcher and a COLUMNS matcher expand through the same shared nodes.
SELECT t.* APPLY (x -> x, 'p_'), t.* FROM (SELECT 1 AS a) AS t FORMAT TSVWithNames;
SELECT COLUMNS('a') APPLY (x -> x, 'p_'), COLUMNS('a') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> x, 'p_') APPLY toString, * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
-- An identity lambda chained after another prefixed transformer owns a private node, so its
-- accumulated name (`q_p_a`) does not change.
SELECT * APPLY (toString, 'p_') APPLY (x -> x, 'q_'), * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
-- A lambda naming another column redirects every matched column onto that one node, so the
-- prefixed name repeats within the transformer's own expansion.
SELECT * APPLY (x -> a) APPLY (x -> x, 'p_'), * FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
-- `ORDER BY ALL` reads the same resolved-name cache, so the prefixed name must reach the
-- output column.
SELECT * APPLY (x -> x, 'p_') FROM (SELECT 1 AS all) ORDER BY ALL FORMAT TSVWithNames;
-- Controls: an unprefixed chain, a non-identity lambda, untuple and REPLACE each name their
-- own fresh node, and an alias wins over the cached name.
SELECT * APPLY (x -> x), * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> x) APPLY toString, * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (toString, 'f_'), * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> untuple(x), 'p_'), * FROM (SELECT (1, 2) AS a) FORMAT TSVWithNames;
SELECT * REPLACE (a AS a), * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> x, 'p_'), a AS z FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> x, 'p_'), * APPLY (x -> x, 'q_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
-- The prefix survives an EXPLAIN QUERY TREE round-trip.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a)) WHERE explain ILIKE '%f_a%';

-- An INSERT column list expands the transformers outside the query tree, so a named
-- transformer over a bare matcher must be rejected there too.
DROP TABLE IF EXISTS t_04401;
CREATE TABLE t_04401 (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04401 (COLUMNS('.*') APPLY (x -> *, 'p_')) VALUES (1, 2); -- { serverError BAD_ARGUMENTS }
INSERT INTO t_04401 (COLUMNS('.*') APPLY (x -> compound_value.*, 'p_')) VALUES (1, 2); -- { serverError BAD_ARGUMENTS }
INSERT INTO t_04401 (COLUMNS('.*') REPLACE (* AS a)) VALUES (1, 2); -- { serverError BAD_ARGUMENTS }
INSERT INTO t_04401 (COLUMNS('.*') EXCEPT (b)) VALUES (1);
SELECT * FROM t_04401 ORDER BY a FORMAT TSVWithNames;
DROP TABLE t_04401;
