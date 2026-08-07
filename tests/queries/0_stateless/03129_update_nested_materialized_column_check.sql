SET asterisk_include_materialized_columns = 1 ;

CREATE TABLE elements
(
    `id` UInt32,
    `nested.key` Array(String),
    `nested.value` Array(String),
    `nested.key_hashed` Array(UInt64) MATERIALIZED arrayMap(x -> sipHash64(x), nested.key),
    `nested.val_hashed` Array(UInt64) MATERIALIZED arrayMap(x -> sipHash64(x), nested.value),
)
    ENGINE = Memory ;


INSERT INTO elements (id,`nested.key`,`nested.value`) VALUES (5555, ['moto', 'hello'],['chocolatine', 'croissant']);

SELECT * FROM elements ;

ALTER TABLE elements
UPDATE
    `nested.key` = arrayFilter((x, v) -> NOT (match(v, 'chocolatine')), `nested.key`, `nested.value` ),
    `nested.value` = arrayFilter((x, v) -> NOT (match(v, 'chocolatine')), `nested.value`, `nested.value`)
WHERE id = 5555
SETTINGS mutations_sync = 1 ;

SELECT * FROM elements ;

ALTER TABLE elements
UPDATE
    `nested.value` = arrayMap(x -> concat(x, ' au chocolat'), `nested.value`)
WHERE id = 5555
SETTINGS mutations_sync = 1 ;

SELECT * FROM elements ;
