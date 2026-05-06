SELECT value FROM system.settings where name='max_insert_block_size';
SET max_insert_block_size=100000;
SELECT value FROM system.settings where name='max_insert_block_size';
SELECT changed FROM system.settings where name='max_insert_block_size';
SET max_insert_block_size=DEFAULT;
SELECT value FROM system.settings where name='max_insert_block_size';
SELECT changed FROM system.settings where name='max_insert_block_size';
