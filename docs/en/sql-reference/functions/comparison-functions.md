---
sidebar_position: 36
sidebar_label: Comparison
---

# Comparison Functions

Comparison functions always return 0 or 1 (Uint8).

The following types can be compared:

-   numbers
-   strings and fixed strings
-   dates
-   dates with times

within each group, but not between different groups.

For example, you can’t compare a date with a string. You have to use a function to convert the string to a date, or vice versa.

Strings are compared by bytes. A shorter string is smaller than all strings that start with it and that contain at least one more character.

## equals, a = b and a == b operator

## notEquals, a != b and a \<\> b operator

## less, \< operator

## greater, \> operator

## lessOrEquals, \<= operator

## greaterOrEquals, \>= operator

