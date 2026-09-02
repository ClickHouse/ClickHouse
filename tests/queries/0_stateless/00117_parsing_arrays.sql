DROP TABLE IF EXISTS null_00117;
CREATE TABLE null_00117 (a Array(UInt64), b Array(String), c Array(Array(Date))) ENGINE = Memory;

INSERT INTO null_00117 (a) VALUES ([1,2]), ([3, 4]), ([ 5 ,6]), ([	7  ,   8  	  ]), ([]), ([   ]);
INSERT INTO null_00117 (b) VALUES ([ 'Hello' , 'World' ]);
INSERT INTO null_00117 (c) VALUES ([	]), ([ [ ] ]), ([[],[]]), ([['2015-01-01', '2015-01-02'], ['2015-01-03', '2015-01-04']]);

SELECT a, b, c FROM null_00117 ORDER BY a, b, c;

DROP TABLE null_00117;