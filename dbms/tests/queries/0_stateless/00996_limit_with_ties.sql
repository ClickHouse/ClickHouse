DROP TABLE IF EXISTS ties;
CREATE TABLE ties (a Int) ENGINE = Memory;

-- SET experimental_use_processors=1;

INSERT INTO ties VALUES (1), (1), (2), (2), (2), (2) (3), (3);

SELECT a FROM ties order by a limit 1 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 3 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 5 with ties;
SELECT '*';

SET max_block_size = 2;
SELECT a FROM ties order by a limit 1, 1 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 1, 2 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 2 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 2, 3 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 4 with ties;
SELECT '*';

SET max_block_size = 3;
SELECT a FROM ties order by a limit 1 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 2, 3 with ties;
SELECT '*';
SELECT a FROM ties order by a limit 3, 2 with ties;
SELECT '*';

DROP TABLE ties;
