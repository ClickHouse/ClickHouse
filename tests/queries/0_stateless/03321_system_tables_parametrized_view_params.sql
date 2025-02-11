DROP TABLE IF EXISTS raw_data;

DROP TABLE IF EXISTS parametrized_view_one_param;

DROP TABLE IF EXISTS parametrized_view_multiple_params;

CREATE TABLE raw_data (id UInt32, data String) ENGINE = MergeTree ORDER BY id;

CREATE VIEW parametrized_view_one_param AS SELECT * FROM raw_data WHERE id = {id:UInt32};

SELECT name, engine, parametrized_view_paramters
    FROM system.tables
    WHERE database = currentDatabase() and name = 'parametrized_view_one_param';

CREATE VIEW parametrized_view_multiple_params AS SELECT * FROM raw_data WHERE id BETWEEN {id_from:UInt32} AND {id_to:UInt32};

SELECT name, engine, parametrized_view_paramters
    FROM system.tables
    WHERE database = currentDatabase() and name = 'parametrized_view_multiple_params';

DROP TABLE parametrized_view_one_param;

DROP TABLE parametrized_view_multiple_params;

DROP TABLE raw_data;