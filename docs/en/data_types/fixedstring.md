# FixedString(N)

A fixed-length string of N bytes (not characters or code points). N must be a strictly positive natural number.
When the server reads a string that contains fewer bytes (such as when parsing INSERT data), the string is padded to N bytes by appending null bytes at the right.
When the server reads a string that contains more bytes, an error message is returned.
When the server writes a string (such as when outputting the result of a SELECT query), null bytes are not trimmed off of the end of the string, but are output.
Note that this behavior differs from MySQL behavior for the CHAR type (where strings are padded with spaces, and the spaces are removed for output).

Fewer functions can work with the FixedString(N) type than with String, so it is less convenient to use.

