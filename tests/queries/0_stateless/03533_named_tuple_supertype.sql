SET enable_analyzer=1;

CREATE TABLE named_tuples_03533_1 (`a` Tuple(s String, i Int64), `b` Tuple(s String, i Int32)) ENGINE=Memory;

INSERT INTO named_tuples_03533_1 VALUES (('y', 20),('a', 100)), (('x',-10),('b', 10));

SELECT x, toTypeName(x) FROM ( SELECT arrayFilter(x -> ((x.i) > 10), [if(a.i > 0, a, b)]) AS x FROM named_tuples_03533_1 );

CREATE TABLE named_tuples_03533_2 (`a` Tuple(s String, i Int64), `b` Tuple(x String, y Int32)) ENGINE=Memory;

INSERT INTO named_tuples_03533_2 VALUES (('y', 10),('a', 100)), (('x',-10),('b', 10));

SELECT x, toTypeName(x) FROM ( SELECT if(a.i > 0, a, b) AS x FROM named_tuples_03533_2 );
