
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select count(), age group by age),
    projection p2 (select age, name group by age, name)
) ENGINE = MergeTree order by uid;

INSERT INTO users VALUES (1231, 'John', 33);

DELETE FROM users WHERE 1; -- { serverError NOT_IMPLEMENTED }

DELETE FROM users WHERE uid = 1231 SETTINGS lightweight_mutation_projection_mode = 'throw';  -- { serverError NOT_IMPLEMENTED }

DELETE FROM users WHERE uid = 1231 SETTINGS lightweight_mutation_projection_mode = 'drop';

SYSTEM FLUSH LOGS;

-- expecting no projection
SELECT
    name,
    `table`
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

SELECT * FROM users ORDER BY uid;

DROP TABLE users;