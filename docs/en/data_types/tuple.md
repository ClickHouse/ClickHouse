# Tuple(T1, T2, ...)

Tuples can't be written to tables (other than Memory tables). They are used for temporary column grouping. Columns can be grouped when an IN expression is used in a query, and for specifying certain formal parameters of lambda functions. For more information, see "IN operators" and "Higher order functions".

Tuples can be output as the result of running a query. In this case, for text formats other than JSON\*, values are comma-separated in brackets. In JSON\* formats, tuples are output as arrays (in square brackets).

