select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast([0, 0, 1, 1] as Array(Int8)));
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast([-1, -1, 1, 1] as Array(Int8)));
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = 0, 'true' = 1))));
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = -1, 'true' = 1))));
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt8)), [0, 0, 1, 1]);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt16)), [0, 0, 1, 1]);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt32)), [0, 0, 1, 1]);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt64)), [0, 0, 1, 1]);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int8)), [0, 0, 1, 1]);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int16)), [0, 0, 1, 1]);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int32)), [0, 0, 1, 1]);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int64)), [0, 0, 1, 1]);
select arrayROCAUC(cast([-0.1, -0.4, -0.35, -0.8] as Array(Float32)) , [0, 0, 1, 1]);
select arrayROCAUC([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0]);
select arrayROCAUC([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0]);

-- passing scale = true
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], true);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast([0, 0, 1, 1] as Array(Int8)), true);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast([-1, -1, 1, 1] as Array(Int8)), true);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = 0, 'true' = 1))), true);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = -1, 'true' = 1))), true);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt8)), [0, 0, 1, 1], true);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt16)), [0, 0, 1, 1], true);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt32)), [0, 0, 1, 1], true);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt64)), [0, 0, 1, 1], true);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int8)), [0, 0, 1, 1], true);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int16)), [0, 0, 1, 1], true);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int32)), [0, 0, 1, 1], true);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int64)), [0, 0, 1, 1], true);
select arrayROCAUC(cast([-0.1, -0.4, -0.35, -0.8] as Array(Float32)) , [0, 0, 1, 1], true);
select arrayROCAUC([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0], true);
select arrayROCAUC([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0], true);

-- passing scale = false
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], false);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast([0, 0, 1, 1] as Array(Int8)), false);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast([-1, -1, 1, 1] as Array(Int8)), false);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = 0, 'true' = 1))), false);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = -1, 'true' = 1))), false);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt8)), [0, 0, 1, 1], false);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt16)), [0, 0, 1, 1], false);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt32)), [0, 0, 1, 1], false);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt64)), [0, 0, 1, 1], false);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int8)), [0, 0, 1, 1], false);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int16)), [0, 0, 1, 1], false);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int32)), [0, 0, 1, 1], false);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int64)), [0, 0, 1, 1], false);
select arrayROCAUC(cast([-0.1, -0.4, -0.35, -0.8] as Array(Float32)) , [0, 0, 1, 1], false);
select arrayROCAUC([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0], false);
select arrayROCAUC([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0], false);

-- passing offsets as [0, 0, 0, 0]
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast([0, 0, 1, 1] as Array(Int8)), true, [0, 0, 0, 0]);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast([-1, -1, 1, 1] as Array(Int8)), true, [0, 0, 0, 0]);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = 0, 'true' = 1))), true, [0, 0, 0, 0]);
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], cast(['false', 'false', 'true', 'true'] as Array(Enum8('false' = -1, 'true' = 1))), true, [0, 0, 0, 0]);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt8)), [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt16)), [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt32)), [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC(cast([10, 40, 35, 80] as Array(UInt64)), [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int8)), [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int16)), [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int32)), [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC(cast([-10, -40, -35, -80] as Array(Int64)), [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC(cast([-0.1, -0.4, -0.35, -0.8] as Array(Float32)) , [0, 0, 1, 1], true, [0, 0, 0, 0]);
select arrayROCAUC([0, 3, 5, 6, 7.5, 8], [1, 0, 1, 0, 0, 0], true, [0, 0, 0, 0]);
select arrayROCAUC([0.1, 0.35, 0.4, 0.8], [1, 0, 1, 0], true, [0, 0, 0, 0]);

-- alias
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], false);

-- negative tests
select arrayROCAUC([0, 0, 1, 1]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select arrayROCAUC([0.1, 0.35], [0, 0, 1, 1]); -- { serverError BAD_ARGUMENTS }
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], materialize(true)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], [0, 0, 0, 0]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], true, [0, 0, 0, 0], true); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], true, [0, 0, 0, NULL]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], true, ['a', 'b', 'c', 'd']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], true, [0, 1, 0, 0, 0]); -- { serverError BAD_ARGUMENTS }
select arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1], true, [-1, 0, 0, 0]); -- { serverError BAD_ARGUMENTS }
select arrayROCAUC(x, y, true, z) from (
  select [1] as x, [0] as y, [0, 0, 0, 0, 0, 0, 0, 0] as z
  UNION ALL
  select [1] as x, [0] as y, [] as z
); -- { serverError BAD_ARGUMENTS }
select arrayROCAUC(x, y, true, z) from (
  select [1] as x, [0] as y, [0, 0, 0] as z
  UNION ALL
  select [1] as x, [1] as y, [0, 0, 0, 0, 0] as z
); -- { serverError BAD_ARGUMENTS }
