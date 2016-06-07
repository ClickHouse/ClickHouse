drop table if exists aliases_test;

create table aliases_test (
date Date, id UInt64,
array default ['zero','one','two'],
d1 default array,
a1 alias array, a2 alias a1, a3 alias a2,
a4 alias arrayMap(x -> toString(x), range(3)), a5 alias a4, a6 alias a5,
`struct.d1` default array,
`struct.a1` alias array, `struct.a2` alias struct.a1, `struct.a3` alias struct.a2,
`struct.a4` alias arrayMap(x -> toString(x), range(3)), `struct.a5` alias struct.a4, `struct.a6` alias struct.a5
) engine=MergeTree(date, id, 1);

insert into aliases_test (id) values (0);

select '-- Ensure ALIAS columns are not selected by asterisk';
select * from aliases_test;

select '-- select DEFAULT and ALIAS arrays';
select d1, a1, a2, a3, a4, a5, a6 from aliases_test;
select '-- select DEFAULT and ALIAS nested columns';
select struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test;

select d1, a1 from aliases_test array join d1, a1;
select d1, a1 from aliases_test array join d1, a1 as a2;
select d1, a1 from aliases_test array join d1 as d2, a1;
select '-- array join, but request the original columns';
select d1, a1 from aliases_test array join d1 as d2, a1 as a2;

select '-- array join, do not use the result';
select array from aliases_test array join d1, a1;
select array from aliases_test array join d1 as d2, a1 as a1;

select '-- select DEFAULT and ALIAS arrays, array joining one at a time';
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join d1;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a1;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a2;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a3;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a4;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a5;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a6;

select '-- select DEFAULT and ALIAS arrays, array joining one at a time and aliasing result with original name';
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join d1 as d1;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a1 as a1;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a2 as a2;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a3 as a3;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a4 as a4;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a5 as a5;
select array, d1, a1, a2, a3, a4, a5, a6 from aliases_test array join a6 as a6;

select '-- select DEFAULT and ALIAS arrays and array join result, aliased as `joined`';
select array, d1, a1, a2, a3, a4, a5, a6, joined from aliases_test array join d1 as joined;
select array, d1, a1, a2, a3, a4, a5, a6, joined from aliases_test array join a1 as joined;
select array, d1, a1, a2, a3, a4, a5, a6, joined from aliases_test array join a2 as joined;
select array, d1, a1, a2, a3, a4, a5, a6, joined from aliases_test array join a3 as joined;
select array, d1, a1, a2, a3, a4, a5, a6, joined from aliases_test array join a4 as joined;
select array, d1, a1, a2, a3, a4, a5, a6, joined from aliases_test array join a5 as joined;
select array, d1, a1, a2, a3, a4, a5, a6, joined from aliases_test array join a6 as joined;

select '-- select DEFAULT and ALIAS nested columns, array joining one at a time';
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.d1;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a1;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a2;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a3;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a4;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a5;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a6;

select '-- select DEFAULT and ALIAS nested columns, array joining one at a time and aliasing result with original name';
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.d1 as `struct.d1`;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a1 as `struct.a1`;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a2 as `struct.a2`;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a3 as `struct.a3`;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a4 as `struct.a4`;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a5 as `struct.a5`;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct.a6 as `struct.a6`;

select '-- select DEFAULT and ALIAS nested columns and array join result, aliased as `joined`';
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6, joined from aliases_test array join struct.d1 as joined;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6, joined from aliases_test array join struct.a1 as joined;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6, joined from aliases_test array join struct.a2 as joined;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6, joined from aliases_test array join struct.a3 as joined;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6, joined from aliases_test array join struct.a4 as joined;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6, joined from aliases_test array join struct.a5 as joined;
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6, joined from aliases_test array join struct.a6 as joined;

select '-- array join whole nested table';
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct;

select '-- array join whole nested table not using the result';
select array from aliases_test array join struct;

select '-- array join whole nested table, aliasing with original name';
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct as struct;

select '-- array join whole nested table, aliasing with original name not using the result';
select array from aliases_test array join struct as struct;

select '-- array join whole nested table, aliasing as `class`';
select array, class.d1, class.a1, class.a2, class.a3, class.a4, class.a5, class.a6 from aliases_test array join struct as class;

select '-- array join whole nested table, aliasing as `class` and not using the result';
select array from aliases_test array join struct as class;

select '-- array join whole nested table, aliasing as `class` but requesting the original columns';
select array, struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6 from aliases_test array join struct as class;

select array,
struct.d1, struct.a1, struct.a2, struct.a3, struct.a4, struct.a5, struct.a6,
class.d1, class.a1, class.a2, class.a3, class.a4, class.a5, class.a6
from aliases_test array join struct as class;

drop table aliases_test;
