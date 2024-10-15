select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast([0, 0, 1, 1] as Array(Int8)));
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast([-1, -1, 1, 1] as Array(Int8)));
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = 0, 'true' = 1))));
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = -1, 'true' = 1))));
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt8)), [0, 0, 1, 1]);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt16)), [0, 0, 1, 1]);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt32)), [0, 0, 1, 1]);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt64)), [0, 0, 1, 1]);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int8)), [0, 0, 1, 1]);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int16)), [0, 0, 1, 1]);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int32)), [0, 0, 1, 1]);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int64)), [0, 0, 1, 1]);
select arrayAUC(cast([-0.1, -0.4, -0.35, -0.8] as Array(Float32)) , [0, 0, 1, 1]);
select arrayAUC([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0]);
select arrayAUC([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0]);

select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], true);
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast([0, 0, 1, 1] as Array(Int8)), true);
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast([-1, -1, 1, 1] as Array(Int8)), true);
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = 0, 'true' = 1))), true);
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = -1, 'true' = 1))), true);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt8)), [0, 0, 1, 1], true);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt16)), [0, 0, 1, 1], true);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt32)), [0, 0, 1, 1], true);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt64)), [0, 0, 1, 1], true);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int8)), [0, 0, 1, 1], true);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int16)), [0, 0, 1, 1], true);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int32)), [0, 0, 1, 1], true);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int64)), [0, 0, 1, 1], true);
select arrayAUC(cast([-0.1, -0.4, -0.35, -0.8] as Array(Float32)) , [0, 0, 1, 1], true);
select arrayAUC([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0], true);
select arrayAUC([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0], true);

select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], false);
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast([0, 0, 1, 1] as Array(Int8)), false);
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast([-1, -1, 1, 1] as Array(Int8)), false);
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = 0, 'true' = 1))), false);
select arrayAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = -1, 'true' = 1))), false);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt8)), [0, 0, 1, 1], false);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt16)), [0, 0, 1, 1], false);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt32)), [0, 0, 1, 1], false);
select arrayAUC(cast([10, 40, 35, 80] as Array(UInt64)), [0, 0, 1, 1], false);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int8)), [0, 0, 1, 1], false);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int16)), [0, 0, 1, 1], false);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int32)), [0, 0, 1, 1], false);
select arrayAUC(cast([-10, -40, -35, -80] as Array(Int64)), [0, 0, 1, 1], false);
select arrayAUC(cast([-0.1, -0.4, -0.35, -0.8] as Array(Float32)) , [0, 0, 1, 1], false);
select arrayAUC([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0], false);
select arrayAUC([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0], false);

-- negative tests
select arrayAUC([0, 0, 1, 1]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select arrayAUC([0.1, 0.35], [0, 0, 1, 1]); -- { serverError BAD_ARGUMENTS }
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], materialize(true)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], true, true); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
