DROP TABLE IF EXISTS ttl_empty_parts;

CREATE TABLE ttl_empty_parts (id UInt32, d Date) ENGINE = MergeTree ORDER BY tuple() PARTITION BY id;

INSERT INTO ttl_empty_parts SELECT 0, toDate('2005-01-01') + number from numbers(500);
INSERT INTO ttl_empty_parts SELECT 1, toDate('2050-01-01') + number from numbers(500);

SELECT count() FROM ttl_empty_parts;
SELECT count() FROM system.parts WHERE table = 'ttl_empty_parts' AND database = currentDatabase() AND active;

ALTER TABLE ttl_empty_parts MODIFY TTL d;

-- To be sure, that task, which clears outdated parts executed.
DETACH TABLE ttl_empty_parts;
ATTACH TABLE ttl_empty_parts;

SELECT count() FROM ttl_empty_parts;
SELECT count() FROM system.parts WHERE table = 'ttl_empty_parts' AND database = currentDatabase() AND active;

DROP TABLE ttl_empty_parts;
