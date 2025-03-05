-- Проверка, что table function URL работает с file:// схемой
DROP TABLE IF EXISTS url_file_test;

CREATE TABLE url_file_test
(
    id UInt32,
    name String
)
ENGINE = Memory;

SYSTEM CREATE FILE '/tmp/test.csv' '1,Alexey\n2,Yuliia';

SELECT *
FROM url('file:///tmp/test.csv', 'CSV', 'id UInt32, name String')
ORDER BY id;

DROP TABLE url_file_test;

