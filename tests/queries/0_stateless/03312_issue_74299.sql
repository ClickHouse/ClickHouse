DROP TABLE if exists t0;

CREATE TABLE t0 (c0 Int) ENGINE = Memory;

INSERT INTO t0 values (1);

SELECT * FROM t0 where 1;

INSERT INTO TABLE t0 (c0) VALUES (INTERVAL currentProfiles() MINUTE); -- { clientError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE if exists t0;