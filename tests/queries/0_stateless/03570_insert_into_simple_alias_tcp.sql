-- TODO:
-- - INSERT via http

SET input_format_skip_unknown_fields=0;
SET insert_allow_alias_columns=1;

DROP TABLE IF EXISTS alias_insert_test;
CREATE TABLE alias_insert_test
(
    -- Base physical columns
    user_id UInt64,
    user_name String,
    rating Float64,

    -- Simple ALIAS columns (insertable)
    uid ALIAS user_id,
    login ALIAS user_name,

    -- Complex ALIAS columns (non-insertable)
    username_normalized ALIAS upper(user_name),
    rating_normalized ALIAS round(rating)
)
ENGINE = MergeTree()
ORDER BY user_id;

-- non-async INSERT
SET async_insert = 0;
INSERT INTO alias_insert_test VALUES (0, 'root', 0.);
INSERT INTO alias_insert_test (user_id, user_name, rating) VALUES (1, 'zeno',    10.);
INSERT INTO alias_insert_test (uid, login, rating)         VALUES (2, 'alice',   20.);
INSERT INTO alias_insert_test (user_id, login, rating)     VALUES (3, 'bob',     30.);
INSERT INTO alias_insert_test (uid, user_name, rating)     VALUES (4, 'charlie', 40.);

-- custom formats
INSERT INTO alias_insert_test FORMAT JSONEachRow {"user_id": 5, "login": "john", "rating": 50.};

INSERT INTO alias_insert_test FORMAT JSONEachRow {"uid": 6, "login": "mike", "rating": 60.};

INSERT INTO alias_insert_test (uid, login, rating) FORMAT JSONEachRow {"uid": 7, "login": "jule", "rating": 70.};

-- Custom formats cannot detect end of query for serverError hint:
-- INSERT INTO alias_insert_test FORMAT JSONEachRow {"uid": 7, "login": "foo", "rating": 70., "user_name": "james"}; -- { serverError DUPLICATE_COLUMN }

-- negative cases
INSERT INTO alias_insert_test (uid, login, rating) SETTINGS insert_allow_alias_columns=0 VALUES (99, 'FAIL', 999.0); -- { serverError NO_SUCH_COLUMN_IN_TABLE }
INSERT INTO alias_insert_test (user_id, username_normalized, rating) VALUES (99, 'FAIL', 999.0); -- { serverError NO_SUCH_COLUMN_IN_TABLE }
INSERT INTO alias_insert_test (user_id, user_name, rating_normalized) VALUES (99, 'FAIL', 999.0); -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- async insert
SET async_insert=1;
INSERT INTO alias_insert_test (user_id, user_name, rating) VALUES (10, 'zeno',  100.);

INSERT INTO alias_insert_test (uid, login, rating)         VALUES (20, 'alice', 200.);

INSERT INTO alias_insert_test FORMAT JSONEachRow {"user_id": 50, "login": "john", "rating": 500.};

INSERT INTO alias_insert_test FORMAT JSONEachRow {"uid": 60, "login": "mike", "rating": 600.};

INSERT INTO alias_insert_test (uid, login, rating) FORMAT JSONEachRow {"uid": 70, "login": "june", "rating": 700.};

INSERT INTO alias_insert_test FORMAT JSONEachRow {"uid": 99, "login": "foo", "rating": 999., "user_name": "james"}; -- { serverError DUPLICATE_COLUMN }

SELECT user_id, user_name, rating FROM alias_insert_test ORDER BY user_id;

DROP TABLE IF EXISTS alias_insert_test;
