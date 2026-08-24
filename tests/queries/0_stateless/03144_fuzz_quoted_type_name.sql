create table t (x 123) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x `a.b`) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x Array(`a.b`)) engine Memory; -- { clientError SYNTAX_ERROR }

create table t (x Array(`ab`)) engine Memory; -- { serverError UNKNOWN_TYPE }
create table t (x `ab`) engine Memory; -- { serverError UNKNOWN_TYPE }
create table t (x `Int64`) engine Memory;