SET mutations_sync = 2;

SELECT '-- Compact parts';

CREATE TABLE tab (id Int64, dflt Int64 DEFAULT 54321) ENGINE MergeTree ORDER BY id;
INSERT INTO tab (id, dflt) VALUES (1, 2);
INSERT INTO tab (id) VALUES (2);
SELECT 'Origin';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab MATERIALIZE COLUMN dflt;
SELECT 'After materialize';
SELECT * FROM tab ORDER BY id;
DROP TABLE tab;

SELECT '-- Wide parts';

CREATE TABLE tab (id Int64, dflt Int64 DEFAULT 54321) ENGINE MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO tab (id, dflt) VALUES (1, 2);
INSERT INTO tab (id) VALUES (2);
SELECT 'Origin';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab MATERIALIZE COLUMN dflt;
SELECT 'After materialize';
SELECT * FROM tab ORDER BY id;
DROP TABLE tab;

SELECT '-- Nullable column != physically absent';

CREATE TABLE tab (id Int64, dflt Nullable(Int64) DEFAULT 54321) ENGINE MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO tab (id, dflt) VALUES (1, 2);
INSERT INTO tab (id, dflt) VALUES (2, NULL);
INSERT INTO tab (id) VALUES (3);
SELECT 'Origin';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab MATERIALIZE COLUMN dflt;
SELECT 'After materialize';
SELECT * FROM tab ORDER BY id;
DROP TABLE tab;

SELECT '-- Parts with renamed column';

CREATE TABLE tab (id Int64, dflt Int64 DEFAULT 54321) ENGINE MergeTree ORDER BY id;
INSERT INTO tab (id, dflt) VALUES (1, 2);
INSERT INTO tab (id) VALUES (2);
SELECT 'Origin';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab RENAME COLUMN dflt TO dflt2;
SELECT 'After rename';
SELECT * FROM tab ORDER BY id;
ALTER TABLE tab MATERIALIZE COLUMN bar;
SELECT 'After materialize';
SELECT * FROM tab ORDER BY id;
DROP TABLE tab;
