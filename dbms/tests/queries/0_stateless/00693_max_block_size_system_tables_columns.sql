SELECT avg(blockSize()) <= 10 FROM system.tables SETTINGS max_block_size = 10;
SELECT avg(blockSize()) <= 10 FROM system.tables LIMIT 10 SETTINGS max_block_size = 10;
SELECT (SELECT count() FROM system.tables SETTINGS max_block_size = 10) = (SELECT count() FROM system.tables SETTINGS max_block_size = 9);
SELECT (SELECT count() FROM system.tables SETTINGS max_block_size = 100) = (SELECT count() FROM system.tables SETTINGS max_block_size = 1000);

CREATE TEMPORARY TABLE t_00693 (x UInt8);
SELECT * FROM system.tables WHERE is_temporary AND name='t_00693';

SELECT avg(blockSize()) <= 10000 FROM system.columns SETTINGS max_block_size = 10;
SELECT avg(blockSize()) <= 10000 FROM system.columns LIMIT 10 SETTINGS max_block_size = 10;
SELECT (SELECT count() FROM system.columns SETTINGS max_block_size = 10) = (SELECT count() FROM system.columns SETTINGS max_block_size = 9);
SELECT (SELECT count() FROM system.columns SETTINGS max_block_size = 100) = (SELECT count() FROM system.columns SETTINGS max_block_size = 1000);
SELECT (SELECT count() FROM system.columns SETTINGS max_block_size = 13) = (SELECT count() FROM system.columns SETTINGS max_block_size = 1000000);
