-- `LOOKUP INDEX` declarations must survive the `clickhouse_json` AST round-trip:
-- `ASTColumns::{writeJSON,readJSON}` carry the separate `lookup_indices` slot and
-- `ASTIndexDeclaration::{writeJSON,readJSON}` carry the `is_lookup_index` flag.

-- CREATE TABLE with lookup indexes round-trips byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (id UInt64, subid UInt64, value String, LOOKUP INDEX idx_set (id, subid) TYPE table_set, LOOKUP INDEX idx_join (id, subid) TYPE table_join) ENGINE = MergeTree ORDER BY (id, subid)'));

-- Mixed with a regular data skipping index: both slots keep their own declarations.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (id UInt64, value String, INDEX skip_idx value TYPE bloom_filter GRANULARITY 4, LOOKUP INDEX idx_set (id) TYPE table_set) ENGINE = MergeTree ORDER BY id'));

-- ALTER TABLE ADD/DROP LOOKUP INDEX round-trips through the JSON dialect.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD LOOKUP INDEX idx (id, subid) TYPE table_join'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP LOOKUP INDEX idx'));
