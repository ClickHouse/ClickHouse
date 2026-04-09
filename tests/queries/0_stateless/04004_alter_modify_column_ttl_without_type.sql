DROP TABLE IF EXISTS alter_modify_column_ttl_without_type;

SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE alter_modify_column_ttl_without_type
(
    uid Int16,
    name String,
    age Date
)
ENGINE = MergeTree
ORDER BY uid;

INSERT INTO alter_modify_column_ttl_without_type VALUES (1, 'expired', '2000-01-01'), (2, 'valid', today());

ALTER TABLE alter_modify_column_ttl_without_type MODIFY COLUMN name TTL age + INTERVAL 1 DAY;
SHOW CREATE TABLE alter_modify_column_ttl_without_type FORMAT TSVRaw;

OPTIMIZE TABLE alter_modify_column_ttl_without_type FINAL;

SELECT uid, name FROM alter_modify_column_ttl_without_type ORDER BY uid;

DROP TABLE alter_modify_column_ttl_without_type;
