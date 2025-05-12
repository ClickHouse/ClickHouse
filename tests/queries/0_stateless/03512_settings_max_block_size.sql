CREATE TABLE tab (column Int) ENGINE = Memory;

INSERT INTO TABLE tab (column) FROM INFILE '/dev/null' SETTINGS max_block_size = 0 FORMAT Values; -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;
