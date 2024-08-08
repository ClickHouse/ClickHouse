-- Test for least/greatest function: if the input has null values, should not return null
select greatest(x1, NULL), least(x1, NULL), greatest(x1, toNullable(4)), least(x1,toNullable(4)), greatest(x1, x2), least(x1,x2), greatest(x1, x2, 4), least(x1,x2, 4) from (select materialize(NULL) as x1, materialize(toFloat32(2)) as x2);
select greatest(x1, NULL), least(x1, NULL), greatest(x1, toNullable(4)), least(x1,toNullable(4)), greatest(x1, x2), least(x1,x2), greatest(x1, x2, 4), least(x1,x2, 4) from (select materialize(1)    as x1, materialize(toFloat32(2)) as x2);

select greatest(x1, NULL), least(x1, NULL), greatest(x1, toNullable(4)), least(x1,toNullable(4)), greatest(x1, x2), least(x1,x2), greatest(x1, x2, 4), least(x1,x2, 4) from (select materialize(NULL) as x1, materialize(toDecimal32(2, 5)) as x2);
select greatest(x1, NULL), least(x1, NULL), greatest(x1, toNullable(4)), least(x1,toNullable(4)), greatest(x1, x2), least(x1,x2), greatest(x1, x2, 4), least(x1,x2, 4) from (select materialize(1)    as x1, materialize(toDecimal32(2, 5)) as x2);

select greatest(toNullable(1), 2), least(toNullable(1), 2), greatest(toNullable(1), 2, NULL), least(toNullable(1), 2, NULL);
select greatest(1, NULL),          least(1, NULL),          greatest(1, 2, NULL),             least(1, 2, NULL);
select greatest(NULL, NULL, NULL), least(NULL, NULL, NULL);