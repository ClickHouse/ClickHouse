-- Multi-part table identifiers fold trailing parts into the table name (DataLakeCatalog namespaces).
SELECT formatQuerySingleLine('SELECT * FROM a.b.c');
SELECT formatQuerySingleLine('SELECT * FROM a.b.c.d');
SELECT formatQuerySingleLine('EXISTS TABLE a.b.c');
SELECT formatQuerySingleLine('SHOW CREATE TABLE a.b.c');
SELECT formatQuerySingleLine('DESCRIBE TABLE a.b.c');
SELECT formatQuerySingleLine('INSERT INTO a.b.c FORMAT Values');
SELECT formatQuerySingleLine('USE a.b');
SELECT formatQuerySingleLine('USE a.b.c');
-- Grants on db.namespace.* must keep the trailing dot so they do not cover e.g. namespace2.
SELECT formatQuerySingleLine('GRANT SELECT ON a.b.* TO test_user');
SELECT formatQuerySingleLine('GRANT SELECT ON a.b.c.* TO test_user');
SELECT formatQuerySingleLine('GRANT SELECT ON a.b.c TO test_user');
SELECT formatQuerySingleLine('SELECT * FROM a.b.'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('USE a.b.'); -- { serverError SYNTAX_ERROR }
