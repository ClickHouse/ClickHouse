-- Compact JSON (default)
EXPLAIN PLAN json=1 SELECT 1;

-- Pretty-printed JSON
EXPLAIN PLAN json=1, pretty=1 SELECT 1;
