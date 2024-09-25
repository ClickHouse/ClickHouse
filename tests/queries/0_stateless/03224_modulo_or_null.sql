select moduloOrNull(0, 0);
select moduloOrNull(1, materialize(0));
select moduloOrNull(1,toNullable(materialize(0)));
select moduloOrNull(0, toNullable(materialize(0)));
select moduloOrNull(0, toNullable(materialize(1)));
select moduloOrNull(10, toNullable(materialize(100)));
select moduloOrNull(10000, toNullable(materialize(93)));
