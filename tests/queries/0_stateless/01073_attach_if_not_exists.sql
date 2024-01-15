-- Tags: no-parallel

CREATE TABLE aine (a Int) ENGINE = Log;
ATTACH TABLE aine; -- { serverError 57 }
ATTACH TABLE IF NOT EXISTS aine;
DETACH TABLE aine;
ATTACH TABLE IF NOT EXISTS aine;
EXISTS TABLE aine;
DROP TABLE aine;
