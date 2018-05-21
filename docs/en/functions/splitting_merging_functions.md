# Functions for splitting and merging strings and arrays

## splitByChar(separator, s)

Splits a string into substrings separated by 'separator'.'separator' must be a string constant consisting of exactly one character.
Returns an array of selected substrings. Empty substrings may be selected if the separator occurs at the beginning or end of the string, or if there are multiple consecutive separators.

## splitByString(separator, s)

The same as above, but it uses a string of multiple characters as the separator. The string must be non-empty.

## arrayStringConcat(arr\[, separator\])

Concatenates the strings listed in the array with the separator.'separator' is an optional parameter: a constant string, set to an empty string by default.
Returns the string.

## alphaTokens(s)

Selects substrings of consecutive bytes from the ranges a-z and A-Z.Returns an array of substrings.

