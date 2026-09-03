SET input_format_values_interpret_expressions = 0;

CREATE TEMPORARY TABLE t (x DateTime('UTC'));
INSERT INTO t VALUES ('2000-01-02 03:04:05'), ('1234567890'), (1111111111);

SELECT x FROM t ORDER BY x;
