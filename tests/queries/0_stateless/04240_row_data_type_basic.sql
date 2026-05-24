-- Tags: no-fasttest
-- Basic round-trip: create a Row(...) column populated via MATERIALIZED from
-- existing columns, INSERT, SELECT both the originals and the wrapper. The
-- wrapper must reproduce the source values 1-for-1 and render as a tuple
-- literal in text formats.

DROP TABLE IF EXISTS row_basic;

CREATE TABLE row_basic (
    id UInt64,
    a String,
    b UInt32,
    c String,
    combined Row(a String, b UInt32, c String) MATERIALIZED (a, b, c)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO row_basic (id, a, b, c) VALUES
    (1, 'alpha', 10, 'x'),
    (2, 'beta',  20, 'y'),
    (3, 'gamma', 30, 'z');

SELECT id, a, b, c FROM row_basic ORDER BY id;
SELECT id, combined FROM row_basic ORDER BY id;

-- Field extraction via __rowElement (used internally by the
-- optimizeUseRowWrappers query-plan rule, but also callable directly).
SELECT id,
       __rowElement(combined, 'a'),
       __rowElement(combined, 'b'),
       __rowElement(combined, 'c')
FROM row_basic ORDER BY id;

DROP TABLE row_basic;
