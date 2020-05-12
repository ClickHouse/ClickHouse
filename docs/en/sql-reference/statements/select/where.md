### WHERE Clause {#select-where}

If there is a WHERE clause, it must contain an expression with the UInt8 type. This is usually an expression with comparison and logical operators.
This expression will be used for filtering data before all other transformations.

If indexes are supported by the database table engine, the expression is evaluated on the ability to use indexes.
