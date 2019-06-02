CREATE DATABASE test;

USE test;

CREATE TABLE fact_cpc_clicks (model_id UInt8) ENGINE = Memory;
CREATE TABLE dim_model (model_id UInt8) ENGINE = Memory;

INSERT INTO fact_cpc_clicks VALUES (1);
INSERT INTO dim_model VALUES (1);

select f.model_id from fact_cpc_clicks as f left join dim_model as d on f.model_id=d.model_id limit 10;

USE default;

select f.model_id from test.fact_cpc_clicks as f left join test.dim_model as d on f.model_id=d.model_id limit 10;
