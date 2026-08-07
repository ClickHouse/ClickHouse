-- Tags: no-parallel

SET send_logs_level = 'fatal';

set allow_deprecated_database_ordinary=1;
DROP DATABASE IF EXISTS 02265_atomic_db;
DROP DATABASE IF EXISTS 02265_ordinary_db;

CREATE DATABASE 02265_atomic_db ENGINE = Atomic;
CREATE DATABASE 02265_ordinary_db ENGINE = Ordinary;

CREATE TABLE 02265_ordinary_db.join_table ( `a` Int64 ) ENGINE = Join(`ALL`, LEFT, a);
INSERT INTO 02265_ordinary_db.join_table VALUES (111);

RENAME TABLE 02265_ordinary_db.join_table TO 02265_atomic_db.join_table;

SELECT * FROM 02265_atomic_db.join_table;

DROP DATABASE IF EXISTS 02265_atomic_db;
DROP DATABASE IF EXISTS 02265_ordinary_db;
