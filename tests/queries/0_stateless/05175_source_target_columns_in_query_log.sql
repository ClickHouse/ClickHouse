-- An `INSERT ... SELECT` lists in `system.query_log.columns` the columns it writes into the target table, next to the
-- columns the SELECT reads from the source. The QueryStart row has them too: they are known before any data moves.
DROP TABLE IF EXISTS tgt;
DROP TABLE IF EXISTS src;

CREATE TABLE tgt (tgtKey UInt32, tgtVal String)
ENGINE = MergeTree ORDER BY tgtKey;

CREATE TABLE src (srcKey UInt32, srcVal String)
ENGINE = MergeTree ORDER BY srcKey
AS
SELECT number, 'val-' || number FROM numbers(10);

INSERT INTO tgt (tgtKey, tgtVal) SELECT * FROM src SETTINGS log_comment = '05175_source_target_columns' ;

SYSTEM FLUSH LOGS query_log;

SELECT tables, columns, type
FROM system.query_log
WHERE query_kind = 'Insert'
  AND current_database = currentDatabase()
  AND log_comment = '05175_source_target_columns'
ORDER BY type
FORMAT VERTICAL;


DROP TABLE tgt;
DROP TABLE src;
