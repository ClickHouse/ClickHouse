-- Tags: no-fasttest
-- The test exercises Parquet, which is not built in fast-test images.

SET allow_experimental_nullable_array_type = 1;

DROP TABLE IF EXISTS nullable_array_format_null_as_default_src;
DROP TABLE IF EXISTS nullable_array_format_null_as_default_dst;

CREATE TABLE nullable_array_format_null_as_default_src (arr Nullable(Array(Int32))) ENGINE = Memory;
INSERT INTO nullable_array_format_null_as_default_src VALUES ([1, 2]), (NULL), ([]), ([3, 4]);

INSERT INTO FUNCTION file('04615_nullable_array_format_null_as_default.parquet', Parquet) SELECT arr FROM nullable_array_format_null_as_default_src SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file('04615_nullable_array_format_null_as_default.arrow', Arrow) SELECT arr FROM nullable_array_format_null_as_default_src SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file('04615_nullable_array_format_null_as_default.arrowstream', ArrowStream) SELECT arr FROM nullable_array_format_null_as_default_src SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file('04615_nullable_array_format_null_as_default.orc', ORC) SELECT arr FROM nullable_array_format_null_as_default_src SETTINGS engine_file_truncate_on_insert = 1;

SET allow_experimental_nullable_array_type = 0;

CREATE TABLE nullable_array_format_null_as_default_dst (arr Array(Int32) DEFAULT [42]) ENGINE = Memory;

TRUNCATE TABLE nullable_array_format_null_as_default_dst;
INSERT INTO nullable_array_format_null_as_default_dst FROM INFILE '04615_nullable_array_format_null_as_default.parquet' SETTINGS input_format_null_as_default = 1, input_format_defaults_for_omitted_fields = 1 FORMAT Parquet;
SELECT 'Parquet', groupArray(arr) FROM nullable_array_format_null_as_default_dst;

TRUNCATE TABLE nullable_array_format_null_as_default_dst;
INSERT INTO nullable_array_format_null_as_default_dst FROM INFILE '04615_nullable_array_format_null_as_default.arrow' SETTINGS input_format_null_as_default = 1, input_format_defaults_for_omitted_fields = 1, input_format_arrow_use_native_reader = 1 FORMAT Arrow;
SELECT 'Arrow native', groupArray(arr) FROM nullable_array_format_null_as_default_dst;

TRUNCATE TABLE nullable_array_format_null_as_default_dst;
INSERT INTO nullable_array_format_null_as_default_dst FROM INFILE '04615_nullable_array_format_null_as_default.arrow' SETTINGS input_format_null_as_default = 1, input_format_defaults_for_omitted_fields = 1, input_format_arrow_use_native_reader = 0 FORMAT Arrow;
SELECT 'Arrow library', groupArray(arr) FROM nullable_array_format_null_as_default_dst;

TRUNCATE TABLE nullable_array_format_null_as_default_dst;
INSERT INTO nullable_array_format_null_as_default_dst FROM INFILE '04615_nullable_array_format_null_as_default.arrowstream' SETTINGS input_format_null_as_default = 1, input_format_defaults_for_omitted_fields = 1, input_format_arrow_use_native_reader = 1 FORMAT ArrowStream;
SELECT 'ArrowStream native', groupArray(arr) FROM nullable_array_format_null_as_default_dst;

TRUNCATE TABLE nullable_array_format_null_as_default_dst;
INSERT INTO nullable_array_format_null_as_default_dst FROM INFILE '04615_nullable_array_format_null_as_default.orc' SETTINGS input_format_null_as_default = 1, input_format_defaults_for_omitted_fields = 1 FORMAT ORC;
SELECT 'ORC', groupArray(arr) FROM nullable_array_format_null_as_default_dst;

DROP TABLE nullable_array_format_null_as_default_dst;
DROP TABLE nullable_array_format_null_as_default_src;
