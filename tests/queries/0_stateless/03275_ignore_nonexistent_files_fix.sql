-- Tags: no-fasttest

SELECT * FROM s3(
        'http://localhost:11111/test/03036_json_archive.zip :: example11.jsonl',
        JSONEachRow,
        'id UInt32, data String'
    )
ORDER BY tuple(*)
SETTINGS s3_ignore_file_doesnt_exist=1, use_cache_for_count_from_files=0;
