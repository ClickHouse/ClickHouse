CREATE TABLE 02356_destination (a Int64, b String) ENGINE = Memory;

INSERT INTO 02356_destination (a, b) SELECT * FROM generateRandom('a Int64, b String') LIMIT 100 SETTINGS max_threads=1, max_block_size=100;
SYSTEM FLUSH LOGS;
SELECT read_rows = written_rows, read_rows = result_rows, read_bytes = written_bytes, read_bytes = result_bytes FROM system.query_log where normalized_query_hash = 1214411238725380014 and type='QueryFinish' and current_database = currentDatabase() FORMAT CSV;