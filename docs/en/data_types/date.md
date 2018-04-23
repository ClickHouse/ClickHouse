# Date

A date. Stored in two bytes as the number of days since 1970-01-01 (unsigned). Allows storing values from just after the beginning of the Unix Epoch to the upper threshold defined by a constant at the compilation stage (currently, this is until the year 2106, but the final fully-supported year is 2105).
The minimum value is output as 0000-00-00.

The date is stored without the time zone.

