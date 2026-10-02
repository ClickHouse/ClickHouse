select 'TEST groupNumericIndexedVector';

DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt32,
    value UInt64
)
ENGINE = MergeTree()
ORDER BY ds;

INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-26', 105, 5), ('2023-12-26', 104, 4), ('2023-12-26', 103, 3);
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-27', 10000001, 7), ('2023-12-27', 10000002, 3);

select numericIndexedVectorShortDebugString(groupNumericIndexedVectorState(uin, value)) from uin_value_details;
select groupNumericIndexedVector(uin, value) from uin_value_details;
select numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(uin, value)) from uin_value_details;
select numericIndexedVectorCardinality(groupNumericIndexedVectorState(uin, value)) from uin_value_details;

select numericIndexedVectorShortDebugString(groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')) from uin_value_details;
select numericIndexedVectorShortDebugString(groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-27')) from uin_value_details;

select numericIndexedVectorCardinality(groupNumericIndexedVectorState('BSI', 64, 0)(uin, value)) from uin_value_details;
select numericIndexedVectorCardinality(groupNumericIndexedVectorState('BSI', 32, 0)(uin, value)) from uin_value_details;
select numericIndexedVectorCardinality(groupNumericIndexedVectorState('BSI', 16, 0)(uin, value)) from uin_value_details;
select numericIndexedVectorCardinality(groupNumericIndexedVectorState('BSI', 32, 14)(uin, value)) from uin_value_details; -- { serverError BAD_ARGUMENTS }
select numericIndexedVectorCardinality(groupNumericIndexedVectorState('RawSum', 32, 14)(uin, value)) from uin_value_details; -- { serverError BAD_ARGUMENTS }
select numericIndexedVectorCardinality(groupNumericIndexedVectorState('BSI', 64, 14)(uin, value)) from uin_value_details; -- { serverError BAD_ARGUMENTS }

with 
  numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])) AS res1, 
  numericIndexedVectorBuild(mapFromArrays(arrayMap(x -> toUInt32(x), [1, 2, 3]), [10.5, 20.3, 30.892])) AS res2
select tuple(
  numericIndexedVectorAllValueSum(res1),
  numericIndexedVectorCardinality(res1),
  toTypeName(res1),
  numericIndexedVectorAllValueSum(res2),
  numericIndexedVectorCardinality(res2),
  toTypeName(res2)
);

