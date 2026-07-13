-- The constant path of `dictGetKeys` reads the dictionary with several streams that pull blocks
-- from a shared counter. When merging the per-stream results we must restore the original block
-- (scan) order, otherwise the user-visible array order would be grouped by worker and therefore
-- scheduler-dependent, and could differ from the single-stream vector path. That would make
-- wrappers such as `materialize` observable for values matching several keys. This test forces
-- many one-row blocks and multiple threads so the constant path is genuinely parallel, then pins
-- its result (order included) to the vector path.

SET max_block_size = 1;
SET max_threads = 8;

DROP DICTIONARY IF EXISTS dict_order;
DROP TABLE IF EXISTS src_order;

CREATE TABLE src_order
(
    id  UInt64,
    grp UInt32
)
ENGINE = Memory;

-- Many keys map to the same attribute value, so a single lookup matches several keys spread
-- across many one-row blocks (max_block_size = 1).
INSERT INTO src_order SELECT number AS id, (number % 3) AS grp FROM numbers(30);

CREATE DICTIONARY dict_order
(
    id  UInt64,
    grp UInt32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src_order'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT 'Constant path equals vector path including order (must all be 1)';
SELECT
    dictGetKeys('dict_order', 'grp', 0) = dictGetKeys('dict_order', 'grp', materialize(toUInt32(0))) AS grp0,
    dictGetKeys('dict_order', 'grp', 1) = dictGetKeys('dict_order', 'grp', materialize(toUInt32(1))) AS grp1,
    dictGetKeys('dict_order', 'grp', 2) = dictGetKeys('dict_order', 'grp', materialize(toUInt32(2))) AS grp2;

SELECT 'Constant path is deterministic across repeated evaluations (must be 1)';
SELECT dictGetKeys('dict_order', 'grp', 1) = dictGetKeys('dict_order', 'grp', 1) AS stable;

SELECT 'Every key matching grp = 1 is returned exactly once (sorted view, must be 1)';
SELECT arraySort(dictGetKeys('dict_order', 'grp', 1)) = arrayMap(i -> 1 + i * 3, range(10)) AS grp1_complete;

DROP DICTIONARY dict_order;
DROP TABLE src_order;
