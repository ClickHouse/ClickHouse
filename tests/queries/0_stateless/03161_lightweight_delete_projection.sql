
SET lightweight_deletes_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS users;


SELECT 'compact part';

CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select count(), age group by age),
    projection p2 (select age, name group by age, name)
) ENGINE = MergeTree order by uid
SETTINGS min_bytes_for_wide_part = 10485760;

INSERT INTO users VALUES (1231, 'John', 33);

SELECT 'testing throw default mode';

ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'throw';

DELETE FROM users WHERE uid = 1231;  -- { serverError SUPPORT_IS_DISABLED }

SELECT 'testing drop mode';
ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'drop';

DELETE FROM users WHERE uid = 1231;

SELECT * FROM users ORDER BY uid;

SYSTEM FLUSH LOGS;

-- all_1_1_0_2
SELECT
    name
FROM system.parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

-- expecting no projection
SELECT
    name, parent_name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

SELECT 'testing rebuild mode';
INSERT INTO users VALUES (6666, 'Ksenia', 48), (8888, 'Alice', 50);

ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'rebuild';

DELETE FROM users WHERE uid = 6666;

SELECT * FROM users ORDER BY uid;

SYSTEM FLUSH LOGS;

-- all_1_1_0_4, all_3_3_0_4
SELECT
    name
FROM system.parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

-- expecting projection p1, p2
SELECT
    name, parent_name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

DROP TABLE users;


SELECT 'wide part';
CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select count(), age group by age),
    projection p2 (select age, name group by age, name)
) ENGINE = MergeTree order by uid
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO users VALUES (1231, 'John', 33);

SELECT 'testing throw default mode';
ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'throw';

DELETE FROM users WHERE uid = 1231;  -- { serverError SUPPORT_IS_DISABLED }

SELECT 'testing drop mode';
ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'drop';

DELETE FROM users WHERE uid = 1231;

SELECT * FROM users ORDER BY uid;

SYSTEM FLUSH LOGS;

-- all_1_1_0_2
SELECT
    name
FROM system.parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

-- expecting no projection
SELECT
    name, parent_name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

SELECT 'testing rebuild mode';
INSERT INTO users VALUES (6666, 'Ksenia', 48), (8888, 'Alice', 50);

ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'rebuild';

DELETE FROM users WHERE uid = 6666;

SELECT * FROM users ORDER BY uid;

SYSTEM FLUSH LOGS;

-- all_1_1_0_4, all_3_3_0_4
SELECT
    name
FROM system.parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

-- expecting projection p1, p2
SELECT
    name, parent_name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);


DROP TABLE users;