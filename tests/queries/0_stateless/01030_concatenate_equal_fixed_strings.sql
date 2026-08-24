SELECT toFixedString('aa' , 2 ) as a, concat(a, a);
SELECT toFixedString('aa' , 2 ) as a, length(concat(a, a));
SELECT toFixedString('aa' , 2 ) as a, toTypeName(concat(a, a));
