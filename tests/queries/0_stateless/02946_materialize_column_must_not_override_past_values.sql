SET mutations_sync = 2;

DROP TABLE IF EXISTS tab;

-- Tests that existing parts which contain a non-default value in columns with DEFAULT expression remain unchanged by MATERIALIZE COLUMN>
SELECT 'DEFAULT expressions';

SELECT '-- Compact parts';

CREATE TABLE tab (id Int64, dflt Int64 DEFAULT 54321) ENGINE MergeTree ORDER BY id;
INSERT INTO tab (id, dflt) VALUES (1, 1);
INSERT INTO tab (id) VALUES (2);
SELECT 'Before materialize';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab MATERIALIZE COLUMN dflt;
SELECT 'After materialize';
SELECT * FROM tab ORDER BY id;
DROP TABLE tab;

SELECT '-- Wide parts';

CREATE TABLE tab (id Int64, dflt Int64 DEFAULT 54321) ENGINE MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO tab (id, dflt) VALUES (1, 1);
INSERT INTO tab (id) VALUES (2);
SELECT 'Before materialize';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab MATERIALIZE COLUMN dflt;
SELECT 'After materialize';
SELECT * FROM tab ORDER BY id;
DROP TABLE tab;

SELECT '-- Nullable column != physically absent';

CREATE TABLE tab (id Int64, dflt Nullable(Int64) DEFAULT 54321) ENGINE MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO tab (id, dflt) VALUES (1, 1);
INSERT INTO tab (id, dflt) VALUES (2, NULL);
INSERT INTO tab (id) VALUES (3);
SELECT 'Before materialize';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab MATERIALIZE COLUMN dflt;
SELECT 'After materialize';
SELECT * FROM tab ORDER BY id;
DROP TABLE tab;

SELECT '-- Parts with renamed column';

CREATE TABLE tab (id Int64, dflt Int64 DEFAULT 54321) ENGINE MergeTree ORDER BY id;
INSERT INTO tab (id, dflt) VALUES (1, 1);
INSERT INTO tab (id) VALUES (2);
SELECT 'Before materialize';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab RENAME COLUMN dflt TO dflt2;
SELECT 'After rename';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab MATERIALIZE COLUMN dflt2;
SELECT 'After materialize';
SELECT * FROM tab ORDER BY id;
DROP TABLE tab;

-- But for columns with MATERIALIZED expression, all existing parts should be rewritten in case a new expression was set in the meantime.
SELECT 'MATERIALIZED expressions';

SELECT '-- Compact parts';

CREATE TABLE tab (id Int64, mtrl Int64 MATERIALIZED 54321) ENGINE MergeTree ORDER BY id;
INSERT INTO tab (id) VALUES (1);
SELECT 'Before materialize';
SELECT id, mtrl FROM tab ORDER BY id;
ALTER TABLE tab MODIFY COLUMN mtrl Int64 MATERIALIZED 65432;
ALTER TABLE tab MATERIALIZE COLUMN mtrl;
SELECT 'After materialize';
SELECT id, mtrl FROM tab ORDER BY id;
DROP TABLE tab;

SELECT '-- Compact parts';

CREATE TABLE tab (id Int64, mtrl Int64 MATERIALIZED 54321) ENGINE MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO tab (id) VALUES (1);
SELECT 'Before materialize';
SELECT id, mtrl FROM tab ORDER BY id;
ALTER TABLE tab MODIFY COLUMN mtrl Int64 MATERIALIZED 65432;
ALTER TABLE tab MATERIALIZE COLUMN mtrl;
SELECT 'After materialize';
SELECT id, mtrl FROM tab ORDER BY id;
DROP TABLE tab;
