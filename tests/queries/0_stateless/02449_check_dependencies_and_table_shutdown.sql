DROP TABLE IF EXISTS table;
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS view;

CREATE TABLE view (id UInt32, value String) ENGINE=ReplicatedMergeTree('/test/2449/{database}', '1') ORDER BY id;
INSERT INTO view VALUES (1, 'v');

CREATE DICTIONARY dict (id UInt32, value String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(host 'localhost' port tcpPort() user 'default' db currentDatabase() table 'view'))
LAYOUT (HASHED()) LIFETIME (MIN 600 MAX 600);

SHOW CREATE dict;

CREATE TABLE table
(
    col MATERIALIZED dictGet(currentDatabase() || '.dict', 'value', toUInt32(1)),
    phys Int
)
ENGINE = MergeTree()
ORDER BY tuple();

SHOW CREATE TABLE table;

SELECT * FROM dictionary('dict');

DROP TABLE view; -- {serverError HAVE_DEPENDENT_OBJECTS}

-- check that table is not readonly
INSERT INTO view VALUES (2, 'a');

DROP DICTIONARY dict; -- {serverError HAVE_DEPENDENT_OBJECTS}

-- check that dictionary was not detached
SELECT * FROM dictionary('dict');
SYSTEM RELOAD DICTIONARY dict;
SELECT * FROM dictionary('dict') ORDER BY id;

DROP TABLE table;
DROP DICTIONARY dict;
DROP TABLE view;
