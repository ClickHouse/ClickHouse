-- {echoOn}

DROP TABLE IF EXISTS t_param_filter;

SET param_a = 3;
SET param_b = 5;

CREATE TABLE t_param_filter
(
    n Int32,
) ENGINE = MergeTree()
ORDER BY n;

INSERT INTO t_param_filter
SELECT number
FROM numbers(10);

SET allow_experimental_analyzer = 1;
SELECT n FROM t_param_filter SETTINGS additional_table_filters = {'t_param_filter': 'n < {b:String}'};
SELECT n FROM t_param_filter WHERE n > {a:String} SETTINGS additional_table_filters = {'t_param_filter': 'n < {b:String}'};

SET allow_experimental_analyzer = 0;
SELECT n FROM t_param_filter SETTINGS additional_table_filters = {'t_param_filter': 'n < {b:String}'};
SELECT n FROM t_param_filter WHERE n > {a:String} SETTINGS additional_table_filters = {'t_param_filter': 'n < {b:String}'};

