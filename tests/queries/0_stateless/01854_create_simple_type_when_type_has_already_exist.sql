create type MyType3 as int;

create type MyType3 as String; -- { serverError 587 }

create type String as int; -- { serverError 587 }

drop type MyType3;
