#pragma once

#include <Core/Field.h>


namespace DB
{

class IDataType;

/** Used to interpret expressions in a set in IN,
  *  and also in the query of the form INSERT ... VALUES ...
  *
  * To work correctly with expressions of the form `1.0 IN (1)` or, for example, `1 IN (1, 2.0, 2.5, -1)` work the same way as `1 IN (1, 2)`.
  * Checks for the compatibility of types, checks values fall in the range of valid values of the type, makes type conversion.
  * If the value does not fall into the range - returns Null.
  */
Field convertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint = nullptr);

/// Does the same, but throws ARGUMENT_OUT_OF_BOUND if value does not fall into the range.
Field convertFieldToTypeOrThrow(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint = nullptr);

}
