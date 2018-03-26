# Comparison functions

Comparison functions always return 0 or 1 (Uint8).

The following types can be compared:

- numbers
- strings and fixed strings
- dates
- dates with times

within each group, but not between different groups.

For example, you can't compare a date with a string. You have to use a function to convert the string to a date, or vice versa.

Strings are compared by bytes. A shorter string is smaller than all strings that start with it and that contain at least one more character.

Note. Up until version 1.1.54134, signed and unsigned numbers were compared the same way as in C++. In other words, you could get an incorrect result in cases like SELECT 9223372036854775807 &gt; -1. This behavior changed in version 1.1.54134 and is now mathematically correct.

## equals, a = b and a == b operator

## notEquals, a ! operator= b and a `<>` b

## less, `< operator`

## greater, `> operator`

## lessOrEquals, `<= operator`

## greaterOrEquals, `>= operator`

