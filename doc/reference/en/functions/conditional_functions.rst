Conditional functions
---------------------

if(cond, then, else), ternary operator cond ? then : else
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Returns 'then' if 'cond != 0', or 'else' if 'cond = 0'.
'cond' must be UInt 8, and 'then' and 'else' must be a type that has the smallest common type.
