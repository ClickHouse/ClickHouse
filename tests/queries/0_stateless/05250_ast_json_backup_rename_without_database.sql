-- `BACKUP TABLE db.t AS t` renames into the current database (empty new database name in the AST); parseQueryToJSON
-- omitted the empty name, formatQueryFromJSON defaulted it to the old database and the rename disappeared, which
-- for RESTORE changes the destination table. Found by the JSON round-trip stage of json_ast_sql_parser_fuzzer.
SELECT formatQueryFromJSON(parseQueryToJSON('BACKUP TABLE d.t AS t TO Disk(''backups'', ''b.zip'')'));
SELECT formatQueryFromJSON(parseQueryToJSON('RESTORE TABLE d.t AS t FROM Disk(''backups'', ''b.zip'')'));
SELECT formatQueryFromJSON(parseQueryToJSON('BACKUP TABLE d.t AS e.t2, TABLE t3 AS t4, DATABASE db AS db2 TO Disk(''backups'', ''b.zip'')'));
SELECT formatQueryFromJSON(parseQueryToJSON('BACKUP TABLE d.t, DATABASE db EXCEPT TABLES a, b TO Disk(''backups'', ''b.zip'')'));
SELECT parseQueryToJSON('BACKUP TABLE d.t AS t TO Disk(''backups'', ''b.zip'')') LIKE '%"new_database_name":""%';
