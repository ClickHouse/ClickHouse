FixedString(N)
--------------

A fixed-length string of N bytes (not characters or code points). N must be a strictly positive natural number.
When server reads a string (as an input passed in INSERT query, for example) that contains fewer bytes, the string is padded to N bytes by appending null bytes at the right.
When server reads a string that contains more bytes, an error message is returned.
When server writes a string (as an output of SELECT query, for example), null bytes are not trimmed off of the end of the string, but are output.
Note that this behavior differs from MySQL behavior for the CHAR type (where strings are padded with spaces, and the spaces are removed for output).

Fewer functions can work with the FixedString(N) type than with String, so it is less convenient to use.
