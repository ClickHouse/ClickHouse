create type MyType3 as int;

create type MyType3 as String; -- { serverError 588 }

create type String as int; -- { serverError 588 }

drop type MyType3;
