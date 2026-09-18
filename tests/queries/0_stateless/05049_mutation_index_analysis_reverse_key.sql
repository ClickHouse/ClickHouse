-- Index analysis of a mutation predicate must respect reverse-sorted key columns.

DROP TABLE IF EXISTS t_mutation_reverse_key;

CREATE TABLE t_mutation_reverse_key (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY (id DESC, v);

INSERT INTO t_mutation_reverse_key SELECT number, number, number FROM numbers(100);
INSERT INTO t_mutation_reverse_key SELECT number, number, number FROM numbers(100, 100);

ALTER TABLE t_mutation_reverse_key DELETE WHERE id < 50 SETTINGS mutations_sync = 2;
SELECT count(), min(id), max(id) FROM t_mutation_reverse_key;

ALTER TABLE t_mutation_reverse_key UPDATE w = 0 WHERE id >= 150 SETTINGS mutations_sync = 2;
SELECT count(), sum(w) FROM t_mutation_reverse_key;

DROP TABLE t_mutation_reverse_key;
