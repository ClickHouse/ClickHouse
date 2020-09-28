DROP TABLE IF EXISTS test_00961;

CREATE TABLE test_00961 (d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = MergeTree(d, (a, b), 111);

INSERT INTO test_00961 VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

SELECT 
    name, 
    table, 
    hash_of_all_files, 
    hash_of_uncompressed_files, 
    uncompressed_hash_of_compressed_files
FROM system.parts
WHERE table = 'test_00961' and database = currentDatabase();

DROP TABLE test_00961;

