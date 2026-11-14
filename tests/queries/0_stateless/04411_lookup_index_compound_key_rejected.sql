-- A `LOOKUP INDEX` (`table_set` / `table_join`) is documented to accept only plain top-level columns:
-- the lookup entity is built from top-level physical columns and cannot serve a subcolumn. A compound
-- identifier such as `m.keys` parses as an `ASTIdentifier` with `compound() == true`, so it must be
-- rejected at validation time instead of being accepted as an unusable lookup index. A quoted
-- top-level column name that contains a dot is a single-part identifier and must still be accepted.

SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS t_lookup_compound SYNC;
DROP TABLE IF EXISTS t_lookup_compound_alter SYNC;
DROP TABLE IF EXISTS t_lookup_dotted SYNC;

-- A compound (subcolumn) key is rejected for both lookup index types, on CREATE.
CREATE TABLE t_lookup_compound
(
    id UInt64,
    m Map(String, String),
    LOOKUP INDEX idx (m.keys) TYPE table_join
)
ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE t_lookup_compound
(
    id UInt64,
    m Map(String, String),
    LOOKUP INDEX idx (m.keys) TYPE table_set
)
ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

-- A compound key is also rejected on ALTER ... ADD LOOKUP INDEX.
CREATE TABLE t_lookup_compound_alter
(
    id UInt64,
    m Map(String, String)
)
ENGINE = MergeTree ORDER BY id;

ALTER TABLE t_lookup_compound_alter ADD LOOKUP INDEX idx (m.keys) TYPE table_join; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_lookup_compound_alter ADD LOOKUP INDEX idx (m.keys) TYPE table_set; -- { serverError INCORRECT_QUERY }

-- A quoted top-level column name containing a dot is a plain column and is accepted.
CREATE TABLE t_lookup_dotted
(
    `a.b` UInt64,
    val String,
    LOOKUP INDEX idx (`a.b`) TYPE table_join
)
ENGINE = MergeTree ORDER BY `a.b`;

SELECT 'dotted top-level column accepted';

DROP TABLE t_lookup_compound_alter SYNC;
DROP TABLE t_lookup_dotted SYNC;
