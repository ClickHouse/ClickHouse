-- Tags: no-fasttest

CREATE TABLE t0 (c0 Nullable(Int)) ENGINE = IcebergLocal('/file0'); -- { serverError PATH_ACCESS_DENIED }
