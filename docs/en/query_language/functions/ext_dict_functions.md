<a name="ext_dict_functions"></a>

# Functions for working with external dictionaries

For information on connecting and configuring external dictionaries, see "[External dictionaries](../dicts/external_dicts.md#dicts-external_dicts)".

## dictGetUInt8, dictGetUInt16, dictGetUInt32, dictGetUInt64

## dictGetInt8, dictGetInt16, dictGetInt32, dictGetInt64

## dictGetFloat32, dictGetFloat64

## dictGetDate, dictGetDateTime

## dictGetUUID

## dictGetString

`dictGetT('dict_name', 'attr_name', id)`

- Get the value of the attr_name attribute  from the dict_name dictionary using the 'id' key.`dict_name`  and `attr_name`  are constant strings.`id`must be UInt64.
If there is no `id` key in the dictionary, it returns the default value specified in the dictionary description.

## dictGetTOrDefault

`dictGetT('dict_name', 'attr_name', id, default)`

The same as the `dictGetT` functions, but the default value is taken from the function's last argument.

## dictIsIn

`dictIsIn ('dict_name', child_id, ancestor_id)`

- For the 'dict_name' hierarchical dictionary, finds out whether the 'child_id' key is located inside 'ancestor_id' (or matches 'ancestor_id'). Returns UInt8.

## dictGetHierarchy

`dictGetHierarchy('dict_name', id)`

- For the 'dict_name' hierarchical dictionary, returns an array of dictionary keys starting from 'id' and continuing along the chain of parent elements. Returns Array(UInt64).

## dictHas

`dictHas('dict_name', id)`

- Check whether the dictionary has the key. Returns a UInt8 value equal to 0 if there is no key and 1 if there is a key.

