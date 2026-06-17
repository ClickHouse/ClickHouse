DROP TABLE IF EXISTS t;
CREATE TABLE t (letter String) ENGINE=MergeTree order by () partition by letter;
INSERT INTO t VALUES ('a'), ('a'), ('a'), ('a'),  ('b'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('c');
SELECT anyHeavy(if(letter != 'b', letter, NULL)) FROM t;
