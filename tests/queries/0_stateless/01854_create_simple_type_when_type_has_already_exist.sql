create type MyType3 as int;

create type MyType3 as String; -- { serverError 594 }

create type String as int; -- { serverError 594 }

drop type MyType3;
