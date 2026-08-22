-- Tags: no-fasttest, no-parallel
-- Regression test for issue #111402.
-- makeBackwardCompatible must only collapse bidirectional READ|WRITE to the deprecated
-- source-access form, not unidirectional READ or WRITE. Collapsing a one-direction grant
-- causes replaceDeprecated to widen it back to READ|WRITE on the next deserialization,
-- silently escalating the privilege.

DROP USER IF EXISTS test_u_03728;
CREATE USER test_u_03728;

-- A unidirectional WRITE grant must NOT be collapsed to the deprecated FILE form
-- (which implies both READ and WRITE). SHOW GRANTS should preserve WRITE ON FILE.
GRANT WRITE ON FILE TO test_u_03728;
SHOW GRANTS FOR test_u_03728;
REVOKE WRITE ON FILE FROM test_u_03728;

-- A deprecated full-source grant (POSTGRES) must still collapse for backward compatibility:
-- it round-trips through replaceDeprecated as READ|WRITE ON POSTGRES and
-- makeBackwardCompatible collapses READ|WRITE back to POSTGRES.
GRANT POSTGRES ON *.* TO test_u_03728;
SHOW GRANTS FOR test_u_03728;

DROP USER IF EXISTS test_u_03728;
