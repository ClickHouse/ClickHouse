DROP TABLE IF EXISTS t_nested_modify;

CREATE TABLE t_nested_modify (id UInt64, `n.a` Array(UInt32), `n.b` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_nested_modify VALUES (1, [2], ['aa']);
INSERT INTO t_nested_modify VALUES (2, [44, 55], ['bb', 'cc']);

SELECT id, `n.a`, `n.b`, toTypeName(`n.b`) FROM t_nested_modify ORDER BY id;

ALTER TABLE t_nested_modify MODIFY COLUMN `n.b` String;

SELECT id, `n.a`, `n.b`, toTypeName(`n.b`) FROM t_nested_modify ORDER BY id;

DETACH TABLE t_nested_modify;
ATTACH TABLE t_nested_modify;

SELECT id, `n.a`, `n.b`, toTypeName(`n.b`) FROM t_nested_modify ORDER BY id;

DROP TABLE t_nested_modify;
