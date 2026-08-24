DROP TABLE IF EXISTS alias_2__fuzz_25;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE alias_2__fuzz_25 (`dt` LowCardinality(Date), `col` DateTime, `col2` Nullable(Int256), `colAlias0` Nullable(DateTime64(3)) ALIAS col, `colAlias3` Nullable(Int32) ALIAS col3 + colAlias0, `colAlias1` LowCardinality(UInt16) ALIAS colAlias0 + col2, `colAlias2` LowCardinality(Int32) ALIAS colAlias0 + colAlias1, `col3` Nullable(UInt8)) ENGINE = MergeTree ORDER BY dt;
insert into alias_2__fuzz_25 (dt, col, col2, col3) values ('2020-02-01', 1, 2, 3);
SELECT colAlias0, colAlias2, colAlias3 FROM alias_2__fuzz_25; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE alias_2__fuzz_25;
