SELECT bitHammingDistance(1, 5);
SELECT bitHammingDistance(100, 100000);
SELECT bitHammingDistance(-1, 1);

DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
	n1 UInt8,
	n2 UInt16,
	n3 UInt32,
	n4 UInt64
)ENGINE = Memory();

INSERT INTO defaults VALUES (1, 2, 3, 4) (12, 4345, 435, 1233) (45, 675, 32343, 54566) (90, 784, 9034, 778752);

SELECT bitHammingDistance(4, n1) FROM defaults;
SELECT bitHammingDistance(n2, 100) FROM defaults;
SELECT bitHammingDistance(n3, n4) FROM defaults;

DROP TABLE defaults;
