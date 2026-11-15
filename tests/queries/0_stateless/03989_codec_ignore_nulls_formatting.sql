-- Reproducer for https://github.com/ClickHouse/ClickHouse/issues/97656
-- CODEC elements with IGNORE NULLS / RESPECT NULLS modifiers must keep parentheses in formatting.
SELECT formatQuery('ALTER TABLE t (MODIFY COLUMN id UInt64 CODEC(count() IGNORE NULLS, Delta))');
SELECT formatQuery('ALTER TABLE t (MODIFY COLUMN id UInt64 CODEC(count() RESPECT NULLS, Delta))');
