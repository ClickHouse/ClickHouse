Enum
----

Enum8 or Enum16. A set of enumerated string values that are stored as Int8 or Int16. 

Example:

.. code-block:: sql

    Enum8('hello' = 1, 'world' = 2)

This data type has two possible values - 'hello' and 'world'.

The numeric values must be within -128..127 for ``Enum8`` and -32768..32767 for ``Enum16``. Every member of the enum must also have different numbers. The empty string is a valid value. The numbers do not need to be sequential and can be in any order. The order does not matter.

In memory, the data is stored in the same way as the numeric types ``Int8`` and ``Int16``.
When reading in text format, the string is read and the corresponding numeric value is looked up. An exception will be thrown if it is not found.
When writing in text format, the stored number is looked up and the corresponding string is written out. An exception will be thrown if the number does not correspond to a known value.
In binary format, the information is saved in the same way as ``Int8`` and ``Int16``.
The implicit default value for an Enum is the value having the smallest numeric value.

In ORDER BY, GROUP BY, IN, DISTINCT, etc. Enums behave like the numeric value. e.g. they will be sorted by the numeric value in an ``ORDER BY``. Equality and comparison operators behave like they do on the underlying numeric value.

Enum values cannot be compared to numbers, they must be compared to a string. If the string compared to is not a valid value for the Enum, an exception will be thrown. The ``IN`` operator is supported with the Enum on the left hand side and a set of strings on the right hand side.

Most numeric and string operations are not defined for Enum values, e.g. adding a number to an Enum or concatenating a string to an Enum. However, the toString function can be used to convert the Enum to its string value. Enum values are also convertible to numeric types using the ``toT`` function where ``T`` is a numeric type. When ``T`` corresponds to the enum's underlying numeric type, this conversion is zero-cost.

It is possible to add new members to the ``Enum`` using ``ALTER``. If the only change is to the set of values, the operation will be almost instant. It is also possible to remove members of the Enum using ALTER. Removing members is only safe if the removed value has never been used in the table. As a safeguard, changing the numeric value of a previously defined Enum member will throw an exception.

Using ``ALTER``, it is possible to change an ``Enum8`` to an ``Enum16`` or vice versa - just like changing an ``Int8`` to ``Int16``.
