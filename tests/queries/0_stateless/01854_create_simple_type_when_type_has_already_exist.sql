create type MyType3 as int;

create type MyType3 as String; -- { serverError 590 }

create type String as int; -- { serverError 590 }

drop type MyType3;
