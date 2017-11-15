Functions for working with JSON.
--------------------------------
In Yandex.Metrica, JSON is passed by users as session parameters. There are several functions for working with this JSON. (Although in most of the cases, the JSONs are additionally pre-processed, and the resulting values are put in separate columns in their processed format.) All these functions are based on strong assumptions about what the JSON can be, but they try not to do anything.

The following assumptions are made:
 #. The field name (function argument) must be a constant.
 #. The field name is somehow canonically encoded in JSON. For example, ``visitParamHas('{"abc":"def"}', 'abc') = 1``, but ``visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0``
 #. Fields are searched for on any nesting level, indiscriminately.  If there are multiple matching fields, the first occurrence is used.
 #. JSON doesn't have space characters outside of string literals.

visitParamHas(params, name)
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Checks whether there is a field with the 'name' name.

visitParamExtractUInt(params, name)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Parses UInt64 from the value of the field named 'name'. If this is a string field, it tries to parse a number from the beginning of the string. If the field doesn't exist, or it exists but doesn't contain a number, it returns 0.

visitParamExtractInt(params, name)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The same as for Int64.

visitParamExtractFloat(params, name)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The same as for Float64.

visitParamExtractBool(params, name)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Parses a true/false value. The result is UInt8.

visitParamExtractRaw(params, name)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Returns the value of a field, including separators. 

Examples: 

.. code-block:: text

  visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
  visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'

visitParamExtractString(params, name)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Parses the string in double quotes. The value is unescaped. If unescaping failed, it returns an empty string. 

Examples:

.. code-block:: text

  visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
  visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
  visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
  visitParamExtractString('{"abc":"hello}', 'abc') = ''

Currently, there is no support for code points not from the basic multilingual plane written in the format ``\uXXXX\uYYYY`` (they are converted to CESU-8 instead of UTF-8).
