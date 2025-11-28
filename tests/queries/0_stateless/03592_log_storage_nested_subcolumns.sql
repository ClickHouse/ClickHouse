CREATE TABLE nested_null (c0 Nested(c1 Nullable(Int), c2 Int)) ENGINE = Log();
INSERT INTO nested_null (c0.c1, c0.c2) VALUES ([1], [2]);

CREATE TABLE null_name (c0 Nested(null Int, c2 Int)) ENGINE = Log();
INSERT INTO null_name (c0.null, c0.c2) VALUES ([3], [4]);

CREATE TABLE nested_null_clash (c0 Nested(null Nullable(Int), c2 Int)) ENGINE = Log();
INSERT INTO nested_null_clash (c0.null, c0.c2) VALUES ([5], [6]);

-- { echoOn }
SELECT c0.c1.null, c0.c2 FROM nested_null;
SELECT c0.null, c0.c2 FROM null_name;
SELECT c0.null.null, c0.c2 FROM nested_null_clash;
-- { echoOff }

