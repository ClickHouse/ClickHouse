Encoding functions
--------

hex
~~~~~
Accepts a string, number, date, or date with time. Returns a string containing the argument's hexadecimal representation. Uses uppercase letters A-F. 
Doesn't use ``0x`` prefixes or ``h`` suffixes. 
For strings, all bytes are simply encoded as two hexadecimal numbers. Numbers are converted to big endian ("human readable") format. 
For numbers, older zeros are trimmed, but only by entire bytes. 
For example, ``hex(1) = '01'``. Dates are encoded as the number of days since the beginning of the Unix Epoch. Dates with times are encoded as the number of seconds since the beginning of the Unix Epoch.

unhex(str)
~~~~~~~
Accepts a string containing any number of hexadecimal digits, and returns a string containing the corresponding bytes. Supports both uppercase and lowercase letters A-F. The number of hexadecimal digits doesn't have to be even. If it is odd, the last digit is interpreted as the younger half of the 00-0F byte. If the argument string contains anything other than hexadecimal digits, some implementation-defined result is returned (an exception isn't thrown).
If you want to convert the result to a number, you can use the functions 'reverse' and 'reinterpretAsType'

UUIDStringToNum(str)
~~~~~~~
Accepts a string containing the UUID in the text format (``123e4567-e89b-12d3-a456-426655440000``). Returns a binary representation of the UUID in ``FixedString(16)``.

UUIDNumToString(str)
~~~~~~~~
Accepts a FixedString(16) value containing the UUID in the binary format. Returns a readable string containing the UUID in the text format.

bitmaskToList(num)
~~~~~~~
Accepts an integer. Returns a string containing the list of powers of two that total the source number when summed. They are comma-separated without spaces in text format, in ascending order.

bitmaskToArray(num)
~~~~~~~~~
Accepts an integer. Returns an array of UInt64 numbers containing the list of powers of two that total the source number when summed. Numbers in the array are in ascending order.
