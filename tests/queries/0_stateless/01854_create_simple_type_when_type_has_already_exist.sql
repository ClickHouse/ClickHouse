create type MyType3 as int;

create type MyType3 as String; -- { serverError 582 }

create type String as int; -- { serverError 582 }
