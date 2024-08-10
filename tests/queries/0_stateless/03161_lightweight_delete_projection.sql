-- For cloud version, should also consider min_bytes_for_full_part_storage since packed storage exists,
-- but for less redundancy, just let CI test the parameter.

SET lightweight_deletes_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS users_compact;


SELECT 'compact part';

CREATE TABLE users_compact (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select count(), age group by age),
    projection p2 (select age, name group by age, name)
) ENGINE = MergeTree order by uid
SETTINGS min_bytes_for_wide_part = 10485760;

INSERT INTO users_compact VALUES (1231, 'John', 33);

SELECT 'testing throw default mode';

-- { echoOn }

ALTER TABLE users_compact MODIFY SETTING lightweight_mutation_projection_mode = 'throw';

DELETE FROM users_compact WHERE uid = 1231;  -- { serverError SUPPORT_IS_DISABLED }

SELECT 'testing drop mode';
ALTER TABLE users_compact MODIFY SETTING lightweight_mutation_projection_mode = 'drop';

DELETE FROM users_compact WHERE uid = 1231;

SELECT * FROM users_compact ORDER BY uid;

SYSTEM FLUSH LOGS;

-- all_1_1_0_2
SELECT
    name
FROM system.parts
WHERE (database = currentDatabase()) AND (`table` = 'users_compact') AND (active = 1);

-- expecting no projection
SELECT
    name, parent_name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users_compact') AND (active = 1);

SELECT 'testing rebuild mode';
INSERT INTO users_compact VALUES (6666, 'Ksenia', 48), (8888, 'Alice', 50);

ALTER TABLE users_compact MODIFY SETTING lightweight_mutation_projection_mode = 'rebuild';

DELETE FROM users_compact WHERE uid = 6666;

SELECT * FROM users_compact ORDER BY uid;

SYSTEM FLUSH LOGS;

-- all_1_1_0_4, all_3_3_0_4
SELECT
    name
FROM system.parts
WHERE (database = currentDatabase()) AND (`table` = 'users_compact') AND (active = 1);

-- expecting projection p1, p2
SELECT
    name, parent_name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users_compact') AND (active = 1) AND parent_name like 'all_3_3%';

-- { echoOff }

DROP TABLE users_compact;


SELECT 'wide part';
CREATE TABLE users_wide (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select count(), age group by age),
    projection p2 (select age, name group by age, name)
) ENGINE = MergeTree order by uid
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO users_wide VALUES (1231, 'John', 33);

SELECT 'testing throw default mode';

-- { echoOn }

ALTER TABLE users_wide MODIFY SETTING lightweight_mutation_projection_mode = 'throw';

DELETE FROM users_wide WHERE uid = 1231;  -- { serverError SUPPORT_IS_DISABLED }

SELECT 'testing drop mode';
ALTER TABLE users_wide MODIFY SETTING lightweight_mutation_projection_mode = 'drop';

DELETE FROM users_wide WHERE uid = 1231;

SELECT * FROM users_wide ORDER BY uid;

SYSTEM FLUSH LOGS;

-- all_1_1_0_2
SELECT
    name
FROM system.parts
WHERE (database = currentDatabase()) AND (`table` = 'users_wide') AND (active = 1);

-- expecting no projection
SELECT
    name, parent_name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users_wide') AND (active = 1);

SELECT 'testing rebuild mode';
INSERT INTO users_wide VALUES (6666, 'Ksenia', 48), (8888, 'Alice', 50);

ALTER TABLE users_wide MODIFY SETTING lightweight_mutation_projection_mode = 'rebuild';

DELETE FROM users_wide WHERE uid = 6666;

SELECT * FROM users_wide ORDER BY uid;

SYSTEM FLUSH LOGS;

-- all_1_1_0_4, all_3_3_0_4
SELECT
    name
FROM system.parts
WHERE (database = currentDatabase()) AND (`table` = 'users_wide') AND (active = 1);

-- expecting projection p1, p2
SELECT
    name, parent_name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users_wide') AND (active = 1) AND parent_name like 'all_3_3%';

-- { echoOff }

DROP TABLE users_wide;
