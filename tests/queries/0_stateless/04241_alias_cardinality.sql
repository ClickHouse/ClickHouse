-- `CARDINALITY` is a case-insensitive alias of `length`.
-- It originates from the SQL standard / PostgreSQL where the operator is array-only; in ClickHouse it is a full alias
-- of `length` and inherits its behavior for non-array arguments too.
SELECT CARDINALITY([1, 2, 3, 4]);
SELECT cardinality([]);
SELECT cardinality(['a', 'b']);
