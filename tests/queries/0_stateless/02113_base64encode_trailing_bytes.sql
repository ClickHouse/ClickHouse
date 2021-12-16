-- Tags: no-fasttest
SET log_queries=1;

DROP TABLE IF EXISTS tabl_1;
DROP TABLE IF EXISTS tabl_2;

CREATE TABLE tabl_1 (key String) ENGINE MergeTree ORDER BY key;
CREATE TABLE tabl_2 (key String) ENGINE MergeTree ORDER BY key;
SELECT * FROM tabl_1 SETTINGS log_comment = 'ad15a651';
SELECT * FROM tabl_2 SETTINGS log_comment = 'ad15a651';
SYSTEM FLUSH LOGS;

SELECT base64Decode(base64Encode(normalizeQuery(query)))
    FROM system.query_log
<<<<<<< HEAD
<<<<<<< HEAD
    WHERE type = 'QueryFinish' AND log_comment = 'ad15a651' AND current_database = currentDatabase()
=======
    WHERE type = 'QueryFinish' AND log_comment = 'ad15a651'
>>>>>>> e4cab544ab (Fix fast test)
=======
    WHERE type = 'QueryFinish' AND log_comment = 'ad15a651' AND current_database = currentDatabase()
>>>>>>> c148718e73 (Fix fast test)
    GROUP BY normalizeQuery(query)
    ORDER BY normalizeQuery(query);

DROP TABLE tabl_1;
DROP TABLE tabl_2;
