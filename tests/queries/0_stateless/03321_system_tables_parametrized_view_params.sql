DROP TABLE IF EXISTS raw_data;

DROP TABLE IF EXISTS raw_temporary_data;

DROP TABLE IF EXISTS parameterized_view_one_param;

DROP TABLE IF EXISTS parameterized_view_multiple_params;

DROP TABLE IF EXISTS parameterized_view_one_param_temporary;

DROP TABLE IF EXISTS parameterized_view_multiple_params_temporary;

SELECT '-----------------------------------------';

SELECT 'TEST TABLE';

CREATE TABLE raw_data (id UInt32, data String) ENGINE = MergeTree ORDER BY id;

CREATE VIEW parameterized_view_one_param AS SELECT * FROM raw_data WHERE id = {id:UInt32};

SELECT name, engine, parameterized_view_parameters
    FROM system.tables
    WHERE database = currentDatabase() and name = 'parameterized_view_one_param';

CREATE VIEW parameterized_view_multiple_params AS SELECT * FROM raw_data WHERE id BETWEEN {id_from:UInt32} AND {id_to:UInt32};

SELECT name, engine, parameterized_view_parameters
    FROM system.tables
    WHERE database = currentDatabase() and name = 'parameterized_view_multiple_params';

SELECT '-----------------------------------------';

SELECT 'TEST TEMPORARY TABLE';

CREATE TEMPORARY TABLE raw_temporary_data (id UInt32, data String);

CREATE TEMPORARY TABLE alter_test (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32);

CREATE VIEW parameterized_view_one_param_temporary AS SELECT * FROM raw_data WHERE id = {id:UInt32};

SELECT name, engine, parameterized_view_parameters
    FROM system.tables
    WHERE database = currentDatabase() and name = 'parameterized_view_one_param_temporary';

CREATE VIEW parameterized_view_multiple_params_temporary
    AS SELECT * FROM raw_data
    WHERE CounterID BETWEEN {counter_id_from:UInt32} AND {counter_id_to:UInt32}
    AND StartDate BETWEEN {date_from:Date} AND {date_to:UInt32}
    and UserId = {UserId:UInt32};

SELECT name, engine, parameterized_view_parameters
    FROM system.tables
    WHERE database = currentDatabase() and name = 'parameterized_view_multiple_params_temporary';

DROP TABLE parameterized_view_one_param;

DROP TABLE parameterized_view_multiple_params;

DROP TABLE parameterized_view_one_param_temporary;

DROP TABLE parameterized_view_multiple_params_temporary;

DROP TABLE raw_temporary_data;

DROP TABLE raw_data;
