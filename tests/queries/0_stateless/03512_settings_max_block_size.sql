CREATE TABLE tab (column Int) ENGINE = Memory;

SELECT 'Set to zero.';
INSERT INTO TABLE tab (column) FROM INFILE '/dev/null' SETTINGS max_block_size = 0 FORMAT Values; -- { clientError BAD_ARGUMENTS }
SELECT count() FROM numbers(10) AS a, numbers(11) AS b, numbers(12) AS c SETTINGS max_block_size = 0; -- { clientError BAD_ARGUMENTS }

DROP TABLE tab;
