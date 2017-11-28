Functions for working with external dictionaries
------------------------------------------------
For more information, see the section "External dictionaries".

dictGetUInt8, dictGetUInt16, dictGetUInt32, dictGetUInt64
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dictGetInt8, dictGetInt16, dictGetInt32, dictGetInt64
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dictGetFloat32, dictGetFloat64
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dictGetDate, dictGetDateTime
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dictGetUUID
~~~~~~~~~~~

dictGetString
~~~~~~~~~~~~~
``dictGetT('dict_name', 'attr_name', id)``
Gets the value of the 'attr_name' attribute from the 'dict_name' dictionary by the 'id' key.
'dict_name' and 'attr_name' are constant strings.
'id' must be UInt64.
If the 'id' key is not in the dictionary, it returns the default value set in the dictionary definition.

dictGetTOrDefault
~~~~~~~~~~~~~~~~~
``dictGetT('dict_name', 'attr_name', id, default)``
Similar to the functions dictGetT, but the default value is taken from the last argument of the function.

dictIsIn
~~~~~~~~
``dictIsIn('dict_name', child_id, ancestor_id)``
For the 'dict_name' hierarchical dictionary, finds out whether the 'child_id' key is located inside 'ancestor_id' (or matches 'ancestor_id'). Returns UInt8.

dictGetHierarchy
~~~~~~~~~~~~~~~~
``dictGetHierarchy('dict_name', id)``
For the 'dict_name' hierarchical dictionary, returns an array of dictionary keys starting from 'id' and continuing along the chain of parent elements. Returns Array(UInt64).

dictHas
~~~~~~~
``dictHas('dict_name', id)``
check the presence of a key in the dictionary. Returns a value of type UInt8, equal to 0, if there is no key and 1 if there is a key.
