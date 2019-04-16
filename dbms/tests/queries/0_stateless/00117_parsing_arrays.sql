DROP TABLE IF EXISTS null;
CREATE TABLE null (a Array(UInt64), b Array(String), c Array(Array(Date))) ENGINE = Memory;

INSERT INTO null (a) VALUES ([1,2]), ([3, 4]), ([ 5 ,6]), ([	7  ,   8  	  ]), ([]), ([   ]);
INSERT INTO null (b) VALUES ([ 'Hello' , 'World' ]);
INSERT INTO null (c) VALUES ([	]), ([ [ ] ]), ([[],[]]), ([['2015-01-01', '2015-01-02'], ['2015-01-03', '2015-01-04']]);

SELECT a, b, c FROM null ORDER BY a, b, c;

DROP TABLE null;