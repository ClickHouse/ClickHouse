# Functions for working with strings

## empty

Returns 1 for an empty string or 0 for a non-empty string.
The result type is UInt8.
A string is considered non-empty if it contains at least one byte, even if this is a space or a null byte.
The function also works for arrays.

## notEmpty

Returns 0 for an empty string or 1 for a non-empty string.
The result type is UInt8.
The function also works for arrays.

## length

Returns the length of a string in bytes (not in characters, and not in code points).
The result type is UInt64.
The function also works for arrays.

## lengthUTF8

Returns the length of a string in Unicode code points (not in characters), assuming that the string contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it doesn't throw an exception).
The result type is UInt64.

## lower

Converts ASCII Latin symbols in a string to lowercase.

## upper

Converts ASCII Latin symbols in a string to uppercase.

## lowerUTF8

Converts a string to lowercase, assuming the string contains a set of bytes that make up a UTF-8 encoded text.
It doesn't detect the language. So for Turkish the result might not be exactly correct.
If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.
If the string contains a set of bytes that is not UTF-8, then the behavior is undefined.

## upperUTF8

Converts a string to uppercase, assuming the string contains a set of bytes that make up a UTF-8 encoded text.
It doesn't detect the language. So for Turkish the result might not be exactly correct.
If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.
If the string contains a set of bytes that is not UTF-8, then the behavior is undefined.

## reverse

Reverses the string (as a sequence of bytes).

## reverseUTF8

Reverses a sequence of Unicode code points, assuming that the string contains a set of bytes representing a UTF-8 text. Otherwise, it does something else (it doesn't throw an exception).

## concat(s1, s2, ...)

Concatenates the strings listed in the arguments, without a separator.

## substring(s, offset, length)

Returns a substring starting with the byte from the 'offset' index that is 'length' bytes long. Character indexing starts from one (as in standard SQL). The 'offset' and 'length' arguments must be constants.

## substringUTF8(s, offset, length)

The same as 'substring', but for Unicode code points. Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text. If this assumption is not met, it returns some result (it doesn't throw an exception).

## appendTrailingCharIfAbsent(s, c)

If the 's' string is non-empty and does not contain the 'c' character at the end, it appends the 'c' character to the end.

## convertCharset(s, from, to)

Returns the string 's' that was converted from the encoding in 'from' to the encoding in 'to'.

