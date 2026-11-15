DROP TABLE IF EXISTS a SYNC;
CREATE TABLE a (x Int64, y Int64 MATERIALIZED 1 SETTINGS (max_compress_block_size = 30000)) ENGINE = MergeTree ORDER BY x;

-- In cases when the type is not present in column declaration, the parser interprets TTL/COLLATE/SETTINGS as a data type,
-- thus such queries doesn't throw syntax error on client side, just fails to parse. For server side validation these
-- queries still result in an exception of syntax error. Even though the exception is throw for a different reason, they
-- are good safe guards for the future where the parsing of such properties might change.
SELECT 'REMOVE';
ALTER TABLE a MODIFY COLUMN y Int64 REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y DEFAULT 2 REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y MATERIALIZED 3 REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y EPHEMERAL 4 REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y COMMENT '5' REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y CODEC(ZSTD) REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y STATISTICS(tdigest) REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y PRIMARY KEY REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }

SELECT 'The same, but with type';
ALTER TABLE a MODIFY COLUMN y Int64 DEFAULT 2 REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 MATERIALIZED 3 REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 EPHEMERAL 4 REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 COMMENT '5' REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 CODEC(ZSTD) REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 STATISTICS(tdigest) REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 TTL toDate('2025-01-01') + toIntervalDay(x) REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 COLLATE binary REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 SETTINGS (max_compress_block_size = 20000) REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 PRIMARY KEY REMOVE MATERIALIZED; -- { clientError SYNTAX_ERROR }

SELECT 'MODIFY SETTING';
ALTER TABLE a MODIFY COLUMN y Int64 MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y DEFAULT 2 MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y MATERIALIZED 3 MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y EPHEMERAL 4 MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y COMMENT '5' MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y CODEC(ZSTD) MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y STATISTICS(tdigest) MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y PRIMARY KEY MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }

SELECT 'The same, but with type';
ALTER TABLE a MODIFY COLUMN y Int64 DEFAULT 2 MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 MATERIALIZED 3 MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 EPHEMERAL 4 MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 COMMENT '5' MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 CODEC(ZSTD) MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 STATISTICS(tdigest) MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 TTL toDate('2025-01-01') + toIntervalDay(x) MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 COLLATE binary MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 SETTINGS (some_setting = 2) MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 PRIMARY KEY MODIFY SETTING max_compress_block_size = 20000; -- { clientError SYNTAX_ERROR }

SELECT 'RESET SETTING';
ALTER TABLE a MODIFY COLUMN y Int64 RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y DEFAULT 2 RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y MATERIALIZED 3 RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y EPHEMERAL 4 RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y COMMENT '5' RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y CODEC(ZSTD) RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y STATISTICS(tdigest) RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y PRIMARY KEY RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }

SELECT 'The same, but with type';
ALTER TABLE a MODIFY COLUMN y Int64 DEFAULT 2 RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 MATERIALIZED 3 RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 EPHEMERAL 4 RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 COMMENT '5' RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 CODEC(ZSTD) RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 STATISTICS(tdigest) RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 TTL toDate('2025-01-01') + toIntervalDay(x) RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 COLLATE binary RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 SETTINGS (some_setting = 2) RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }
ALTER TABLE a MODIFY COLUMN y Int64 PRIMARY KEY RESET SETTING max_compress_block_size; -- { clientError SYNTAX_ERROR }



SELECT 'All the above, but on server side';

SELECT 'REMOVE';
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y DEFAULT 2 REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y MATERIALIZED 3 REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y EPHEMERAL 4 REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y COMMENT \'5\' REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y CODEC(ZSTD) REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y STATISTICS(tdigest) REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y TTL toDate(\'2025-01-01\') + toIntervalDay(x) REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y COLLATE binary REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y SETTINGS (max_compress_block_size = 20000) REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y PRIMARY KEY REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }

SELECT 'The same, but with type';
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 DEFAULT 2 REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 MATERIALIZED 3 REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 EPHEMERAL 4 REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 COMMENT \'5\' REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 CODEC(ZSTD) REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 STATISTICS(tdigest) REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 TTL toDate(\'2025-01-01\') + toIntervalDay(x) REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 COLLATE binary REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 SETTINGS (max_compress_block_size = 20000) REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 PRIMARY KEY REMOVE MATERIALIZED'); -- { serverError SYNTAX_ERROR }

SELECT 'MODIFY SETTING';
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y DEFAULT 2 MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y MATERIALIZED 3 MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y EPHEMERAL 4 MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y COMMENT \'5\' MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y CODEC(ZSTD) MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y STATISTICS(tdigest) MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y TTL toDate(\'2025-01-01\') + toIntervalDay(x) MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y COLLATE binary MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y SETTINGS (some_setting = 2) MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y PRIMARY KEY MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }

SELECT 'The same, but with type';
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 DEFAULT 2 MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 MATERIALIZED 3 MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 EPHEMERAL 4 MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 COMMENT \'5\' MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 CODEC(ZSTD) MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 STATISTICS(tdigest) MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 TTL toDate(\'2025-01-01\') + toIntervalDay(x) MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 COLLATE binary MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 SETTINGS (some_setting = 2) MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 PRIMARY KEY MODIFY SETTING max_compress_block_size = 20000'); -- { serverError SYNTAX_ERROR }

SELECT 'RESET SETTING';
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y DEFAULT 2 RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y MATERIALIZED 3 RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y EPHEMERAL 4 RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y COMMENT \'5\' RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y CODEC(ZSTD) RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y STATISTICS(tdigest) RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y TTL toDate(\'2025-01-01\') + toIntervalDay(x) RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y COLLATE binary RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y SETTINGS (some_setting = 2) RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y PRIMARY KEY RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }

SELECT 'The same, but with type';
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 DEFAULT 2 RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 MATERIALIZED 3 RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 EPHEMERAL 4 RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 COMMENT \'5\' RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 CODEC(ZSTD) RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 STATISTICS(tdigest) RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 TTL toDate(\'2025-01-01\') + toIntervalDay(x) RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 COLLATE binary RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 SETTINGS (some_setting = 2) RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a MODIFY COLUMN y Int64 PRIMARY KEY RESET SETTING max_compress_block_size'); -- { serverError SYNTAX_ERROR }
