create type MyType3 as int;

create type MyType3 as String; -- { serverError 49 }
