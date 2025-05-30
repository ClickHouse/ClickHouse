DROP TABLE IF EXISTS mytable;

CREATE TABLE mytable (x Int32) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO mytable VALUES (5);

SELECT arrayJoin(['columns.txt', 'count.txt', 'default_compression_codec.txt']) AS name_in_part,
       readFileFromDisk(disk_name, concat(path, name_in_part)) AS file_contents
FROM system.parts
WHERE table='mytable' AND database=currentDatabase()
ORDER BY name_in_part;

DROP TABLE mytable;
