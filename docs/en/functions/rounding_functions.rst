Rounding functions
----------------

floor(x[, N])
~~~~~~~
Returns a rounder number that is less than or equal to 'x'.
A round number is a multiple of 1 / 10N, or the nearest number of the appropriate data type ``if 1 / 10N`` isn't exact.
'N' is an integer constant, optional parameter. By default it is zero, which means to round to an integer.
'N' may be negative.

Examples: ``floor(123.45, 1) = 123.4, floor(123.45, -1) = 120``.

'x' is any numeric type. The result is a number of the same type.
For integer arguments, it makes sense to round with a negative 'N' value (for non-negative 'N', the function doesn't do anything).
If rounding causes overflow (for example, ``floor(-128, -1))``, an implementation-specific result is returned.

ceil(x[, N])
~~~~~~
Returns the smallest round number that is greater than or equal to 'x'. In every other way, it is the same as the 'floor' function (see above)..

round(x[, N])
~~~~~~~
Returns the round number nearest to 'num', which may be less than, greater than, or equal to 'x'.
If 'x' is exactly in the middle between the nearest round numbers, one of them is returned (implementation-specific).
The number '-0.' may or may not be considered round (implementation-specific).
In every other way, this function is the same as 'floor' and 'ceil' described above.

roundToExp2(num)
~~~~~~~~
Accepts a number. If the number is less than one, it returns 0. Otherwise, it rounds the number down to the nearest (whole non-negative) degree of two.

roundDuration(num)
~~~~~~~~
Accepts a number. If the number is less than one, it returns 0. Otherwise, it rounds the number down to numbers from the set: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. This function is specific to Yandex.Metrica and used for implementing the report on session length.

roundAge(num)
~~~~~~~
Accepts a number. If the number is less than 18, it returns 0. Otherwise, it rounds the number down to numbers from the set: 18, 25, 35, 45, 55. This function is specific to Yandex.Metrica and used for implementing the report on user age.
