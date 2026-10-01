DROP TABLE IF EXISTS join_rls;
DROP TABLE IF EXISTS probe_join_rls;
DROP ROW POLICY IF EXISTS join_rls_policy ON join_rls;

CREATE TABLE join_rls (key UInt64, value String) ENGINE = Join(ANY, LEFT, key);
INSERT INTO join_rls VALUES (1, 'a'), (2, 'b');

CREATE TABLE probe_join_rls (key UInt64) ENGINE = TinyLog;
INSERT INTO probe_join_rls VALUES (1), (2), (3);

SELECT '-- without a policy';
SELECT p.key, j.value FROM probe_join_rls AS p LEFT ANY JOIN join_rls AS j ON j.key = p.key ORDER BY p.key;
SELECT joinGet(join_rls, 'value', toUInt64(2)), joinGetOrNull(join_rls, 'value', toUInt64(3));

CREATE ROW POLICY join_rls_policy ON join_rls FOR SELECT USING value = 'a' TO CURRENT_USER;

SELECT '-- with a policy';
SELECT key, value FROM join_rls ORDER BY key;
SELECT p.key, j.value FROM probe_join_rls AS p LEFT ANY JOIN join_rls AS j ON j.key = p.key ORDER BY p.key; -- { serverError ACCESS_DENIED }
SELECT p.key, j.value FROM probe_join_rls AS p LEFT ANY JOIN join_rls AS j ON j.key = p.key ORDER BY p.key SETTINGS join_algorithm = 'hash'; -- { serverError ACCESS_DENIED }
SELECT joinGet(join_rls, 'value', toUInt64(2)); -- { serverError ACCESS_DENIED }
SELECT joinGetOrNull(join_rls, 'value', toUInt64(2)); -- { serverError ACCESS_DENIED }

DROP ROW POLICY join_rls_policy ON join_rls;

SELECT '-- after dropping the policy';
SELECT p.key, j.value FROM probe_join_rls AS p LEFT ANY JOIN join_rls AS j ON j.key = p.key ORDER BY p.key;
SELECT joinGet(join_rls, 'value', toUInt64(2)), joinGetOrNull(join_rls, 'value', toUInt64(3));

DROP TABLE join_rls;
DROP TABLE probe_join_rls;
