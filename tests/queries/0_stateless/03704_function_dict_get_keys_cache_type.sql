-- This test checks whether the used cache only valid per query.
-- This test should fail if the used cache is shared across queries.

DROP DICTIONARY IF EXISTS colors;
DROP TABLE IF EXISTS dict_src;

CREATE TABLE dict_src
(
    id  UInt64,
    grp String
) ENGINE = Memory;

INSERT INTO dict_src VALUES (1, 'blue');

CREATE DICTIONARY colors
(
    id  UInt64,
    grp String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_src'))
LAYOUT(HASHED())
LIFETIME(0);

SELECT dictGetKeys('colors', 'grp', 'blue') AS keys
FROM numbers(1);

TRUNCATE TABLE dict_src;
INSERT INTO dict_src VALUES (2, 'blue');

SYSTEM RELOAD DICTIONARY colors;

SELECT dictGetKeys('colors', 'grp', 'blue') AS keys
FROM numbers(1);

DROP DICTIONARY IF EXISTS colors;
DROP TABLE IF EXISTS dict_src;
