DROP TABLE IF EXISTS alter_enum_array;

CREATE TABLE alter_enum_array(
    Key UInt64,
    Value Array(Enum8('Option1'=1, 'Option2'=2))
)
ENGINE=MergeTree()
ORDER BY tuple();

INSERT INTO alter_enum_array VALUES (1, ['Option2', 'Option1']), (2, ['Option1']);

ALTER TABLE alter_enum_array MODIFY COLUMN Value  Array(Enum8('Option1'=1, 'Option2'=2, 'Option3'=3)) SETTINGS mutations_sync=2;

INSERT INTO alter_enum_array VALUES (3, ['Option1','Option3']);

SELECT * FROM alter_enum_array ORDER BY Key;

DETACH TABLE alter_enum_array;
ATTACH TABLE alter_enum_array;

SELECT * FROM alter_enum_array ORDER BY Key;

OPTIMIZE TABLE alter_enum_array FINAL;

SELECT COUNT() FROM system.mutations where table='alter_enum_array' and database=currentDatabase();

DROP TABLE IF EXISTS alter_enum_array;
