create table a (x `Null`); -- { clientError SYNTAX_ERROR }
create table a (x f(`Null`)); -- { clientError SYNTAX_ERROR }
create table a (x Enum8(f(`Null`, 'World', 2))); -- { clientError SYNTAX_ERROR }
create table a (`value2` Enum8('Hello' = 1, equals(`Null`, 'World', 2), '!' = 3)); -- { clientError SYNTAX_ERROR }

create table a (x Int8) engine Memory;
create table b empty as a;
