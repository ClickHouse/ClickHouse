-- Tags: no-parallel

set allow_deprecated_database_ordinary=1;
DROP DATABASE IF EXISTS _02265_atomic_db;
DROP DATABASE IF EXISTS _02265_ordinary_db;

CREATE DATABASE _02265_atomic_db ENGINE = Atomic;
CREATE DATABASE _02265_ordinary_db ENGINE = Ordinary;

CREATE TABLE _02265_ordinary_db.join_table ( `a` Int64 ) ENGINE = Join(`ALL`, LEFT, a);
INSERT INTO _02265_ordinary_db.join_table VALUES (111);

RENAME TABLE _02265_ordinary_db.join_table TO _02265_atomic_db.join_table;

SELECT * FROM _02265_atomic_db.join_table;

DROP DATABASE IF EXISTS _02265_atomic_db;
DROP DATABASE IF EXISTS _02265_ordinary_db;
