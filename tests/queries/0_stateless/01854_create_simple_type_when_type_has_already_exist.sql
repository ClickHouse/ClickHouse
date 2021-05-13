create type MyType3 as int;

create type MyType3 as String; -- { serverError 585 }

create type String as int; -- { serverError 585 }
