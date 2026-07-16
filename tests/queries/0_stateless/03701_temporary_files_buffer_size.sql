CREATE TABLE t0 (c0 Int) ENGINE = Memory();
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT c0 FROM t0 GROUP BY c0
SETTINGS max_bytes_before_external_group_by = 1
    , temporary_files_buffer_size = 0
    , group_by_two_level_threshold = 1; -- { clientError BAD_ARGUMENTS }
