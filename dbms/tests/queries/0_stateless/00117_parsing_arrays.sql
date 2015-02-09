DROP TABLE IF EXISTS test.null;
CREATE TABLE test.null (a Array(UInt64), b Array(String), c Array(Array(Date))) ENGINE = Memory;

INSERT INTO test.null (a) VALUES ([1,2]), ([3, 4]), ([ 5 ,6]), ([	7  ,   8  	  ]), ([]), ([   ]);
INSERT INTO test.null (b) VALUES ([ 'Hello' , 'World' ]);
INSERT INTO test.null (c) VALUES ([	]), ([ [ ] ]), ([[],[]]), ([['2015-01-01', '2015-01-02'], ['2015-01-03', '2015-01-04']]);

SELECT a, b, c FROM test.null ORDER BY a, b, c;

DROP TABLE test.null;