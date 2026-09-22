DROP TABLE IF EXISTS join_rls;
DROP TABLE IF EXISTS join_rls_late;
DROP TABLE IF EXISTS probe_join_rls;
DROP ROW POLICY IF EXISTS join_rls_policy ON join_rls_late;

CREATE TABLE join_rls (key UInt64, value String) ENGINE = Join(ANY, LEFT, key);
INSERT INTO join_rls VALUES (1, 'a'), (2, 'b');

CREATE TABLE probe_join_rls (key UInt64) ENGINE = TinyLog;
INSERT INTO probe_join_rls VALUES (1), (2), (3);

SELECT '-- no row policy on a Join table';
CREATE ROW POLICY join_rls_policy ON join_rls FOR SELECT USING value = 'a' TO CURRENT_USER; -- { serverError BAD_ARGUMENTS }
SELECT p.key, j.value FROM probe_join_rls AS p LEFT ANY JOIN join_rls AS j ON j.key = p.key ORDER BY p.key;

SELECT '-- a policy created before the table';
CREATE ROW POLICY join_rls_policy ON join_rls_late FOR SELECT USING value = 'a' TO CURRENT_USER;
CREATE TABLE join_rls_late (key UInt64, value String) ENGINE = Join(ANY, LEFT, key);
INSERT INTO join_rls_late VALUES (1, 'a'), (2, 'b');
SELECT key, value FROM join_rls_late ORDER BY key;
SELECT p.key, j.value FROM probe_join_rls AS p LEFT ANY JOIN join_rls_late AS j ON j.key = p.key ORDER BY p.key; -- { serverError NOT_IMPLEMENTED }
SELECT p.key, j.value FROM probe_join_rls AS p LEFT ANY JOIN join_rls_late AS j ON j.key = p.key ORDER BY p.key SETTINGS join_algorithm = 'hash'; -- { serverError NOT_IMPLEMENTED }
SELECT joinGet(join_rls_late, 'value', toUInt64(2)); -- { serverError NOT_IMPLEMENTED }
SELECT joinGetOrNull(join_rls_late, 'value', toUInt64(2)); -- { serverError NOT_IMPLEMENTED }

DROP ROW POLICY join_rls_policy ON join_rls_late;
SELECT p.key, j.value FROM probe_join_rls AS p LEFT ANY JOIN join_rls_late AS j ON j.key = p.key ORDER BY p.key;
SELECT joinGet(join_rls_late, 'value', toUInt64(2)), joinGetOrNull(join_rls_late, 'value', toUInt64(3));

DROP TABLE join_rls;
DROP TABLE join_rls_late;
DROP TABLE probe_join_rls;
