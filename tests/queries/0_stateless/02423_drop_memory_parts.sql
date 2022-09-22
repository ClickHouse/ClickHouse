DROP TABLE IF EXISTS tt;

CREATE TABLE tt
(
    `id` UInt64,
    `value` UInt64
)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS min_bytes_for_wide_part=1000, min_bytes_for_compact_part=900;

SELECT 'init state';
INSERT INTO tt SELECT intDiv(number, 10), number   FROM numbers(30);

SELECT count() FROM tt;
SELECT name, part_type, rows active from system.parts
WHERE table='tt' AND database=currentDatabase();

SELECT 'drop part 0';
ALTER TABLE tt DROP PARTITION 0;

SELECT count() FROM tt;
SELECT name, part_type, rows active from system.parts
WHERE table='tt' AND database=currentDatabase();

SELECT 'detach table';
DETACH TABLE tt;

SELECT name, part_type, rows active from system.parts
WHERE table='tt' AND database=currentDatabase();

SELECT 'attach table';
ATTACH TABLE tt;

SELECT count() FROM tt;
SELECT name, part_type, rows active from system.parts
WHERE table='tt' AND database=currentDatabase();


