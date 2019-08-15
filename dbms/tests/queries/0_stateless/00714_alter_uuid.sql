SELECT '00000000-0000-01f8-9cb8-cb1b82fb3900' AS str, toUUID(str);
SELECT toFixedString('00000000-0000-02f8-9cb8-cb1b82fb3900', 36) AS str, toUUID(str);

SELECT '00000000-0000-03f8-9cb8-cb1b82fb3900' AS str, CAST(str, 'UUID');
SELECT toFixedString('00000000-0000-04f8-9cb8-cb1b82fb3900', 36) AS str, CAST(str, 'UUID');

DROP TABLE IF EXISTS uuid;
CREATE TABLE IF NOT EXISTS uuid
(
    created_at DateTime,
    id0 String,
    id1 FixedString(36)
)
ENGINE = MergeTree
PARTITION BY toDate(created_at)
ORDER BY (created_at);

INSERT INTO uuid VALUES ('2018-01-01 01:02:03', '00000000-0000-05f8-9cb8-cb1b82fb3900', '00000000-0000-06f8-9cb8-cb1b82fb3900');

ALTER TABLE uuid MODIFY COLUMN id0 UUID;
ALTER TABLE uuid MODIFY COLUMN id1 UUID;

SELECT id0, id1 FROM uuid;
SELECT toTypeName(id0), toTypeName(id1) FROM uuid;

DROP TABLE uuid;

-- with UUID in key

CREATE TABLE IF NOT EXISTS uuid
(
    created_at DateTime,
    id0 String,
    id1 FixedString(36)
)
ENGINE = MergeTree
PARTITION BY toDate(created_at)
ORDER BY (created_at, id0, id1);

SET send_logs_level = 'none';

ALTER TABLE uuid MODIFY COLUMN id0 UUID; -- { serverError 44 }
ALTER TABLE uuid MODIFY COLUMN id1 UUID; -- { serverError 44 }

DROP TABLE uuid;
