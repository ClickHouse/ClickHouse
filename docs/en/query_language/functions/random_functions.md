# Functions for generating pseudo-random numbers

Non-cryptographic generators of pseudo-random numbers are used.

All the functions accept zero arguments or one argument.
If an argument is passed, it can be any type, and its value is not used for anything.
The only purpose of this argument is to prevent common subexpression elimination, so that two different instances of the same function return different columns with different random numbers.

## rand

Returns a pseudo-random UInt32 number, evenly distributed among all UInt32-type numbers.
Uses a linear congruential generator.

## rand64

Returns a pseudo-random UInt64 number, evenly distributed among all UInt64-type numbers.
Uses a linear congruential generator.

