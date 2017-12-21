Date
----

A date. Stored in two bytes as the number of days since 1970-01-01 (unsigned). Allows storing values from just after the beginning of the Unix Epoch to the upper threshold defined by a constant at the compilation stage. Currently, this is until the year 2106 (the year 2016 is not covered).
The minimum value is output as 0000-00-00.

The date is stored without the time zone.
