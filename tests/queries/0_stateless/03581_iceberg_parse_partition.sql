-- Tags: no-fasttest

CREATE TABLE t0 (c0 Nullable(Int)) ENGINE = IcebergLocal('/file0') PARTITION BY (`c0.null` IS NULL);  -- { serverError BAD_ARGUMENTS }
