-- A credential in a table's own `SETTINGS` clause must be hidden by `system.table_settings` and
-- `SHOW TABLE SETTINGS`, as `SHOW CREATE TABLE` already hides it.
--
-- `URL` keeps no settings struct, so its settings come from the base implementation reading the
-- stored `CREATE` query rather than from an enumeration. Redaction must not depend on which of the
-- two produced the row, and once did: the base implementation returned the value unmasked and the
-- column that says whether a row was redacted was left empty, so the credential was printed in
-- full - to any user with `SHOW TABLES` on the table, whether or not they may see secrets.

DROP TABLE IF EXISTS with_secrets;
CREATE TABLE with_secrets (a String) ENGINE = URL('http://localhost:1/', CSV)
    SETTINGS url_base = 'http://user:hunter2@host/',
             s3_base = 'https://bucket/f.csv?X-Amz-Signature=abcdef',
             format_avro_schema_registry_url = 'http://user:hunter2@registry/';

SELECT '-- the credential is hidden, and the row says so';
-- Only the credential: the host is kept, which is what makes the masked value still useful.
SELECT name, value, is_masked FROM system.table_settings
WHERE database = currentDatabase() AND table = 'with_secrets' ORDER BY name;

SELECT '-- and through the statement too';
SHOW TABLE SETTINGS FROM with_secrets;

SELECT '-- nothing that is not a credential is touched';
CREATE TABLE no_secrets (a String) ENGINE = URL('http://localhost:1/', CSV) SETTINGS url_base = 'http://host/';
SELECT name, value, is_masked FROM system.table_settings
WHERE database = currentDatabase() AND table = 'no_secrets';

DROP TABLE with_secrets;
DROP TABLE no_secrets;
