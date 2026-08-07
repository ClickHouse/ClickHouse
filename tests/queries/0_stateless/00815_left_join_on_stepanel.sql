
DROP TABLE IF EXISTS fact_cpc_clicks;
DROP TABLE IF EXISTS dim_model;

CREATE TABLE fact_cpc_clicks (model_id UInt8) ENGINE = Memory;
CREATE TABLE dim_model (model_id UInt8) ENGINE = Memory;

INSERT INTO fact_cpc_clicks VALUES (1);
INSERT INTO dim_model VALUES (1);

select f.model_id from fact_cpc_clicks as f left join dim_model as d on f.model_id=d.model_id limit 10;

USE default;

select f.model_id from {CLICKHOUSE_DATABASE:Identifier}.fact_cpc_clicks as f left join {CLICKHOUSE_DATABASE:Identifier}.dim_model as d on f.model_id=d.model_id limit 10;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
