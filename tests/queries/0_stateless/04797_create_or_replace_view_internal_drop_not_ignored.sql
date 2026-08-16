-- Tags: no-ordinary-database
-- no-ordinary-database: on Atomic the replaced object is dropped under an internal temporary name.

-- A View and a Dictionary keep no data on disk and support no TRUNCATE, so the DROP that
-- CREATE OR REPLACE issues internally for the replaced object must not be rewritten by
-- ignore_drop_queries_probability.

DROP VIEW IF EXISTS v_04797;
DROP DICTIONARY IF EXISTS d_04797;

CREATE VIEW v_04797 AS SELECT 1 AS x;
CREATE DICTIONARY d_04797 (k UInt64, v String) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);

SET ignore_drop_queries_probability = 1;

CREATE OR REPLACE VIEW v_04797 AS SELECT 2 AS x;
SELECT x FROM v_04797 ORDER BY x;

CREATE OR REPLACE DICTIONARY d_04797 (k UInt64, v String) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(MIN 3 MAX 7);
SYSTEM RELOAD DICTIONARY d_04797;
SELECT lifetime_min, lifetime_max FROM system.dictionaries WHERE database = currentDatabase() AND name = 'd_04797';

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

SET ignore_drop_queries_probability = 0;
DROP VIEW v_04797;
DROP DICTIONARY d_04797;
