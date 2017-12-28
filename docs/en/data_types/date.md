# Date

Date. Stored in two bytes as the number of days since 1970-01-01 (unsigned). Allows storing values from just after the beginning of the Unix Epoch to the upper threshold defined by a constant at the compilation stage (currently, this is until the year 2038, but it may be expanded to 2106).
The minimum value is output as 0000-00-00.

The date is stored without the time zone.

