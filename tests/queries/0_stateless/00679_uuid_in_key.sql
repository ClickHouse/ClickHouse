CREATE TABLE IF NOT EXISTS uuid
(
    created_at DateTime,
    id UUID
)
ENGINE = MergeTree
PARTITION BY toDate(created_at)
ORDER BY (created_at, id);

INSERT INTO uuid (created_at, id) VALUES ('2018-01-01 01:02:03', '00000000-0000-03f8-9cb8-cb1b82fb3900');

SELECT count() FROM uuid WHERE id =  '00000000-0000-03f8-9cb8-cb1b82fb3900';
SELECT count() FROM uuid WHERE id != '00000000-0000-03f8-9cb8-cb1b82fb3900';
SELECT count() FROM uuid WHERE id <  '00000000-0000-03f8-9cb8-cb1b82fb3900';
SELECT count() FROM uuid WHERE id >  '00000000-0000-03f8-9cb8-cb1b82fb3900';
SELECT count() FROM uuid WHERE id <= '00000000-0000-03f8-9cb8-cb1b82fb3900';
SELECT count() FROM uuid WHERE id >= '00000000-0000-03f8-9cb8-cb1b82fb3900';

DROP TABLE uuid;
