create table t (x 123) engine Memory; -- { clientError 62 }
create table t (x `a.b`) engine Memory; -- { clientError 62 }
create table t (x Array(`a.b`)) engine Memory; -- { clientError 62 }

create table t (x Array(`ab`)) engine Memory; -- { serverError 50 }
create table t (x `ab`) engine Memory; -- { serverError 50 }
create table t (x `Int64`) engine Memory;