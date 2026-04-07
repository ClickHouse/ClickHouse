set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE mt (d Date, x String) ENGINE = MergeTree(d, x, 8192);
INSERT INTO mt VALUES ('2106-02-07', 'Hello'), ('1970-01-01', 'World');

SELECT 'Q1', * FROM mt WHERE d = '2106-02-07';
SELECT 'Q2', * FROM mt WHERE d = '1970-01-01';

DETACH TABLE mt;
ATTACH TABLE mt;

SELECT 'Q1', * FROM mt WHERE d = '2106-02-07';
SELECT 'Q2', * FROM mt WHERE d = '1970-01-01';

DROP TABLE mt;
