DROP TABLE IF EXISTS users;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree order by uid;

INSERT INTO users VALUES (1231, 'John', 33),(6666, 'John', 48), (8888, 'John', 50);

SELECT age FROM remote('127.0.0.{2,3}', currentDatabase(), users) group by age limit 20 format JSON SETTINGS output_format_write_statistics=0;

DROP TABLE users;
