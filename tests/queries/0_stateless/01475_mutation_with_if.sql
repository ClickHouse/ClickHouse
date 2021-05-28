DROP TABLE IF EXISTS mutation_table;
CREATE TABLE mutation_table (
    id int,
    price Nullable(Int32)
)
ENGINE = MergeTree()
PARTITION BY id
ORDER BY id;

INSERT INTO mutation_table (id, price) VALUES (1, 100);

ALTER TABLE mutation_table UPDATE price = 150 WHERE id = 1 SETTINGS mutations_sync = 2;

SELECT * FROM mutation_table;

DROP TABLE IF EXISTS mutation_table;
