-- `SYSTEM DROP DATABASE REPLICA ... WITH TABLES` must survive a format -> parse round-trip.
-- The clause is gated by the parser on the `DROP DATABASE REPLICA` form itself, not on a parsed
-- database name, so it is also accepted after `FROM ZKPATH`. The formatter therefore has to emit
-- it outside the `FROM DATABASE` branch; emitting it only there silently dropped the flag for the
-- `FROM ZKPATH` form, and emitting it in both places printed `WITH TABLES` twice.
-- The rewrite-rule matcher folds `with_tables` into the tree hash, so either bug breaks matching
-- and trips the debug-build AST consistency check.

SELECT formatQuerySingleLine('SYSTEM DROP DATABASE REPLICA \'r\' FROM DATABASE db WITH TABLES');
SELECT formatQuerySingleLine('SYSTEM DROP DATABASE REPLICA \'r\' FROM ZKPATH \'/p\' WITH TABLES');

-- Without the clause the flag must stay unset (no stray `WITH TABLES`):
SELECT formatQuerySingleLine('SYSTEM DROP DATABASE REPLICA \'r\' FROM DATABASE db');
SELECT formatQuerySingleLine('SYSTEM DROP DATABASE REPLICA \'r\' FROM ZKPATH \'/p\'');
