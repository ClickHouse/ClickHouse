DROP TABLE IF EXISTS t_index_empty_part;

CREATE TABLE t_index_empty_part (c0 Int, c1 Int)
ENGINE = MergeTree() PRIMARY KEY (c0, c1)
SETTINGS primary_key_lazy_load = 0, remove_empty_parts = 0;

INSERT INTO TABLE t_index_empty_part (c0, c1) VALUES (1, 1);

TRUNCATE t_index_empty_part;
DETACH TABLE t_index_empty_part;
ATTACH TABLE t_index_empty_part;

SELECT rows, primary_key_bytes_in_memory FROM system.parts WHERE database = currentDatabase() AND table = 't_index_empty_part';

DROP TABLE t_index_empty_part;
