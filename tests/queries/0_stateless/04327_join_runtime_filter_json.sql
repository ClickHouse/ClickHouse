SET enable_join_runtime_filters = 1;

-- Value 1 fits in Int8 but JSON stores it as Int64; this was the failing case.
SELECT count() FROM (SELECT '{"a":1}'::JSON AS k) AS t1 JOIN (SELECT '{"a":1}'::JSON AS k) AS t2 USING (k);

-- Value that fits in Int32 but not Int8.
SELECT count() FROM (SELECT '{"a":99999999}'::JSON AS k) AS t1 JOIN (SELECT '{"a":99999999}'::JSON AS k) AS t2 USING (k);

-- String value (was always fine, included for completeness).
SELECT count() FROM (SELECT '{"a":"hello"}'::JSON AS k) AS t1 JOIN (SELECT '{"a":"hello"}'::JSON AS k) AS t2 USING (k);

-- Multiple paths.
SELECT count() FROM (SELECT '{"a":1,"b":"x"}'::JSON AS k) AS t1 JOIN (SELECT '{"a":1,"b":"x"}'::JSON AS k) AS t2 USING (k);

-- Tuple(JSON) join key.
SELECT count() FROM (SELECT tuple('{"a":1}'::JSON) AS k) AS t1 JOIN (SELECT tuple('{"a":1}'::JSON) AS k) AS t2 USING (k);
