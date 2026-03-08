-- Disable force_primary_key_reverse_order: SHOW CREATE output contains ORDER BY which changes with forced DESC
SET force_primary_key_reverse_order = 0;

DROP TABLE IF EXISTS users_02534;
CREATE TABLE users_02534 (id Int16, name String, INDEX bf_idx(name) TYPE minmax) ENGINE=MergeTree ORDER BY id;
SHOW CREATE TABLE users_02534;
DROP TABLE users_02534;

CREATE TABLE users_02534 (id Int16, name String) ENGINE=MergeTree ORDER BY id;
ALTER TABLE users_02534 ADD INDEX bf_idx(name) TYPE minmax;
SHOW CREATE TABLE users_02534;
DROP TABLE users_02534;
