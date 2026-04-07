#pragma once

#include <Core/Field.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class IDataType;

/** Used to interpret expressions in a set in IN,
  *  and also in the query of the form INSERT ... VALUES ...
  *
  * To work correctly with expressions of the form `1.0 IN (1)` or, for example, `1 IN (1, 2.0, 2.5, -1)` work the same way as `1 IN (1, 2)`.
  * Checks for the compatibility of types, checks values fall in the range of valid values of the type, makes type conversion.
  * If the value does not fall into the range - returns Null.
  *
  * When strict_bool is false (default), Bool conversion clamps any non-zero integer to 1:
  *   convertFieldToType(Field(255), Bool) -> Field(true)   i.e. 1
  *   convertFieldToType(Field(256), Bool) -> Field(true)   i.e. 1
  *   convertFieldToType(Field(1),   Bool) -> Field(true)   i.e. 1
  *
  * When strict_bool is true, only 0 and 1 are representable as Bool; other values produce NULL:
  *   convertFieldToType(Field(255), Bool, .., true) -> Field(Null)
  *   convertFieldToType(Field(256), Bool, .., true) -> Field(Null)
  *   convertFieldToType(Field(1),   Bool, .., true) -> Field(true)  i.e. 1
  *
  * This makes the IN operator use exact value semantics for Bool columns:
  *   CAST(1, 'Bool') IN (1)   -> 1  (1 is representable as Bool, matches true)
  *   CAST(1, 'Bool') IN (255) -> 0  (255 is not representable, excluded from set)
  *   CAST(1, 'Bool') IN (256) -> 0  (256 is not representable, excluded from set)
  */
Field convertFieldToType(
    const Field & from_value,
    const IDataType & to_type,
    const IDataType * from_type_hint = nullptr,
    const FormatSettings & format_settings = {},
    bool strict_bool = false);

/// Same as convertFieldToType but returns empty Field in case of an exception.
Field tryConvertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint = nullptr, const FormatSettings & format_settings = {});

/// Does the same, but throws ARGUMENT_OUT_OF_BOUND if value does not fall into the range.
Field convertFieldToTypeOrThrow(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint = nullptr, const FormatSettings & format_settings = {});

/// Applies stricter rules than convertFieldToType, doesn't allow loss of precision converting to Decimal.
/// Returns `Field` if the conversion was successful and the result is equal to the original value, otherwise returns nullopt.
std::optional<Field> convertFieldToTypeStrict(const Field & from_value, const IDataType & from_type, const IDataType & to_type, const FormatSettings & format_settings = {});

}
