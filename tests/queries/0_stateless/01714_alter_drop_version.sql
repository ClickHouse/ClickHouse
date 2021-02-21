DROP TABLE IF EXISTS alter_drop_version;

CREATE TABLE alter_drop_version
(
    `key` UInt64,
    `value` String,
    `ver` Int8
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY key;

INSERT INTO alter_drop_version VALUES (1, '1', 1);

ALTER TABLE alter_drop_version DROP COLUMN ver; --{serverError 524}
ALTER TABLE alter_drop_version RENAME COLUMN ver TO rev; --{serverError 524}

DETACH TABLE alter_drop_version;

ATTACH TABLE alter_drop_version;

SELECT * FROM alter_drop_version;

DROP TABLE IF EXISTS alter_drop_version;
