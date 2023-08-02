select (1, 2) in tuple((1, 2));
select (1, 2) in ((1, 2), (3, 4));
select ((1, 2), (3, 4)) in ((1, 2), (3, 4));
select ((1, 2), (3, 4)) in (((1, 2), (3, 4)));
select ((1, 2), (3, 4)) in tuple(((1, 2), (3, 4)));
select ((1, 2), (3, 4)) in (((1, 2), (3, 4)), ((5, 6), (7, 8)));

select '-';
select 1 in 1;
select 1 in tuple(1);
select tuple(1) in tuple(1);
select tuple(1) in tuple(tuple(1));
select tuple(tuple(1)) in tuple(tuple(1));
select tuple(tuple(1)) in tuple(tuple(tuple(1)));
select tuple(tuple(tuple(1))) in tuple(tuple(tuple(1)));

select '-';
select 1 in Null;
select 1 in tuple(Null);
select 1 in tuple(Null, 1);
select tuple(1) in tuple(tuple(Null));
select tuple(1) in tuple(tuple(Null), tuple(1));
select tuple(tuple(Null), tuple(1)) in tuple(tuple(Null), tuple(1));

select '-';
select 1 in (1 + 1, 1 - 1);
select 1 in (0 + 1, 1, toInt8(sin(5)));
select (0 + 1, 1, toInt8(sin(5))) in (0 + 1, 1, toInt8(sin(5)));
select identity(tuple(1)) in (tuple(1), tuple(2));
select identity(tuple(1)) in (tuple(0), tuple(2));
select identity(tuple(1)) in (identity(tuple(1)), tuple(2));
select identity(tuple(1)) in (identity(tuple(0)), tuple(2));
select identity(tuple(1)) in (identity(tuple(1)), identity(tuple(2)));
select identity(tuple(1)) in (identity(tuple(1)), identity(identity(tuple(2))));
select identity(tuple(1)) in (identity(tuple(0)), identity(identity(tuple(2))));

select '-';
select identity((1, 2)) in (1, 2);
select identity((1, 2)) in ((1, 2), (3, 4));
select identity((1, 2)) in ((1, 2), identity((3, 4)));

select '-';
select (1,2)  as x, ((1,2),(3,4)) as y, 1 in x,  x in y;

select '-';
select 1 in (select 1);
select tuple(1) in (select tuple(1));
select (1, 2) in (select 1, 2);
select (1, 2) in (select (1, 2));
select identity(tuple(1)) in (select tuple(1));
select identity((1, 2)) in (select 1, 2);
select identity((1, 2)) in (select (1, 2));

