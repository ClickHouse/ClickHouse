CREATE TABLE dummy(foo Int64) ENGINE = Memory();
INSERT INTO dummy VALUES (1);
SELECT avgWeighted(100., .1) FROM remote('127.0.0.{2,3}', currentDatabase(), dummy);
SELECT avgWeighted(10, 100) FROM remote('127.0.0.{2,3}', currentDatabase(), dummy);
SELECT avgWeighted(0, 1) FROM remote('127.0.0.{2,3}', currentDatabase(), dummy);
SELECT avgWeighted(0., 0.) FROM remote('127.0.0.{2,3}', currentDatabase(), dummy);
SELECT avgWeighted(1., 0.) FROM remote('127.0.0.{2,3}', currentDatabase(), dummy);
SELECT avgWeighted(toInt8(100), -1) FROM remote('127.0.0.{2,3}', currentDatabase(), dummy);
DROP TABLE dummy;
