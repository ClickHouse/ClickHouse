Comparison functions
--------------------

Comparison functions always return 0 or 1 (Uint8).

The following types can be compared:
 * numbers
 * strings and fixed strings
 * dates
 * dates with times

within each group, but not between different groups.

For example, you can't compare a date with a string. You have to use a function to convert the string to a date, or vice versa.

Strings are compared by bytes. A shorter string is smaller than all strings that start with it and that contain at least one more character.

Note: before version 1.1.54134 signed and unsigned numbers were compared the same way as in C++. That is, you could got an incorrect result in such cases: SELECT 9223372036854775807 > -1. From version 1.1.54134, the behavior has changed and numbers are compared mathematically correct.

equals, a = b and a == b operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

notEquals, a != b and a <> b operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

less, < operator
~~~~~~~~~~~~~~~~

greater, > operator
~~~~~~~~~~~~~~~~~~~

lessOrEquals, <= operator
~~~~~~~~~~~~~~~~~~~~~~~~~

greaterOrEquals, >= operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
