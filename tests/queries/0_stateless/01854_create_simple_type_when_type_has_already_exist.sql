create type MyType3 as int;

create type MyType3 as String; -- { serverError 615 }

create type String as int; -- { serverError 615 }

drop type MyType3;
