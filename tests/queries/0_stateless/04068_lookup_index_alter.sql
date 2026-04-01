SET enable_analyzer = 1;
SET join_algorithm = 'direct,hash';

DROP TABLE IF EXISTS table_lookup_validation SYNC;
DROP TABLE IF EXISTS table_lookup_validation_disabled SYNC;
DROP TABLE IF EXISTS table_lookup_alter_disabled SYNC;
DROP TABLE IF EXISTS table_lookup_alter_dim SYNC;
DROP TABLE IF EXISTS table_lookup_alter_fact SYNC;

CREATE TABLE table_lookup_validation_disabled
(
    id UInt64,
    LOOKUP INDEX idx_set_disabled (id) TYPE table_set
)
ENGINE = MergeTree
ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE table_lookup_alter_disabled
(
    id UInt64
)
ENGINE = MergeTree
ORDER BY id;

ALTER TABLE table_lookup_alter_disabled ADD LOOKUP INDEX idx_set_disabled (id) TYPE table_set; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE table_lookup_alter_disabled SYNC;

SET allow_experimental_lookup_index = 1;

CREATE TABLE table_lookup_validation
(
    id UInt64,
    nullable_id Nullable(UInt64),
    LOOKUP INDEX idx_set_args (id) TYPE table_set(1)
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE table_lookup_validation
(
    id UInt64,
    LOOKUP INDEX idx_set_expr (id + 1) TYPE table_set
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE table_lookup_validation
(
    id UInt64,
    LOOKUP INDEX idx_set_dup (id, id) TYPE table_set
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE table_lookup_validation
(
    id UInt64,
    nullable_id Nullable(UInt64),
    LOOKUP INDEX idx_join_nullable (nullable_id) TYPE table_join
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE table_lookup_validation
(
    id UInt64,
    INDEX idx_old_syntax (id) TYPE table_set GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT formatQuery('CREATE TABLE table_lookup_validation (id UInt64, LOOKUP INDEX idx_granularity (id) TYPE table_set GRANULARITY 1) ENGINE = MergeTree ORDER BY id'); -- { serverError SYNTAX_ERROR }

CREATE TABLE table_lookup_alter_dim
(
    id UInt64,
    subid UInt64
)
ENGINE = MergeTree
ORDER BY (id, subid);

CREATE TABLE table_lookup_alter_fact
(
    id UInt64,
    subid UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY (id, subid);

INSERT INTO table_lookup_alter_dim VALUES (1, 1), (3, 1);
INSERT INTO table_lookup_alter_fact VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c');

ALTER TABLE table_lookup_alter_dim ADD INDEX idx_old_syntax (id, subid) TYPE table_set GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
SELECT formatQuery('ALTER TABLE table_lookup_alter_dim ADD LOOKUP INDEX idx_granularity (id, subid) TYPE table_set GRANULARITY 1'); -- { serverError SYNTAX_ERROR }

ALTER TABLE table_lookup_alter_dim ADD LOOKUP INDEX idx_set (id, subid) TYPE table_set;

SHOW CREATE TABLE table_lookup_alter_dim;

SELECT count()
FROM system.data_skipping_indices
WHERE database = currentDatabase()
    AND table = 'table_lookup_alter_dim';

SELECT id, subid
FROM table_lookup_alter_fact
WHERE (id, subid) IN table_lookup_alter_dim
ORDER BY id, subid;

SELECT '--';

ALTER TABLE table_lookup_alter_dim DROP LOOKUP INDEX idx_set;

SHOW CREATE TABLE table_lookup_alter_dim;

SELECT '--';

ALTER TABLE table_lookup_alter_dim ADD LOOKUP INDEX idx_join (id, subid) TYPE table_join;

SHOW CREATE TABLE table_lookup_alter_dim;

SELECT count()
FROM system.data_skipping_indices
WHERE database = currentDatabase()
    AND table = 'table_lookup_alter_dim';

SELECT
    countIf(explain like '%Algorithm: DirectKeyValueJoin%'),
    countIf(explain like '%ReadFromMergeTree (%table_lookup_alter_dim%)%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT id, subid
    FROM table_lookup_alter_fact
    LEFT ANY JOIN table_lookup_alter_dim
        ON table_lookup_alter_fact.id = table_lookup_alter_dim.id
        AND table_lookup_alter_fact.subid = table_lookup_alter_dim.subid
    ORDER BY id, subid
);

DROP TABLE table_lookup_alter_dim SYNC;
DROP TABLE table_lookup_alter_fact SYNC;
