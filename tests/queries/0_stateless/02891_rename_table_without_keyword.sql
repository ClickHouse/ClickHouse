DROP DATABASE IF EXISTS rename_db;
CREATE DATABASE rename_db;

CREATE TABLE rename_db.r1 (name String) Engine=Memory();
SHOW TABLES FROM rename_db;

RENAME TABLE rename_db.r1 TO rename_db.r2;
SHOW TABLES FROM rename_db;

RENAME rename_db.r2 TO rename_db.r3;
SHOW TABLES FROM rename_db;

DROP DATABASE rename_db;

