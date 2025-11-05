select * from numbers(10, 10) as left right join (select number, number as x, 'str' || number as str, arrayJoin(range(number)) from numbers(10)) as right on left.number = right.number settings enable_lazy_columns_replication=1;

