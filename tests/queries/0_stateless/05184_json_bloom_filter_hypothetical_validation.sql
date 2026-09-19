DROP TABLE IF EXISTS json_bf_hypothetical;
CREATE TABLE json_bf_hypothetical (j JSON(key String)) ENGINE = MergeTree ORDER BY tuple();

CREATE HYPOTHETICAL INDEX hi ON json_bf_hypothetical (j.key) TYPE jsonbf_v1 GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
CREATE HYPOTHETICAL INDEX hi ON json_bf_hypothetical (materialize(j)) TYPE jsonbf_v1 GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
CREATE HYPOTHETICAL INDEX hi ON json_bf_hypothetical (j) TYPE jsonbf_v1 GRANULARITY 1;

DROP TABLE json_bf_hypothetical;
