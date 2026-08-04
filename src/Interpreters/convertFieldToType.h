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
  * When `strict` is false (default), Bool conversion clamps any non-zero integer to 1, and
  * Decimal conversions may silently round/truncate:
  *   convertFieldToType(Field(255), Bool)                 -> Field(true)   i.e. 1
  *   convertFieldToType(Field(256), Bool)                 -> Field(true)   i.e. 1
  *   convertFieldToType(Field(1),   Bool)                 -> Field(true)   i.e. 1
  *   convertFieldToType(Decimal64("33.33"), Decimal64(1)) -> Decimal64("33.3")  (truncated)
  *
  * When `strict` is true, conversions that lose precision return Null instead:
  *   - Bool: only 0 and 1 are representable; other values produce Null:
  *       convertFieldToType(Field(255), Bool, .., true) -> Field(Null)
  *       convertFieldToType(Field(256), Bool, .., true) -> Field(Null)
  *       convertFieldToType(Field(1),   Bool, .., true) -> Field(true)  i.e. 1
  *   - Decimal -> Decimal: rejects any lossy conversion by requiring exact equality after conversion.
  *   - Float64 -> Decimal: converts the Decimal back to Float64 and compares with the original.
  *
  * The strict checks apply recursively inside composite types (Tuple, Array, Map),
  * so e.g. a Tuple(Decimal64(2)) element inside an Array is also checked for precision loss.
  *
  * This makes the IN operator use exact value semantics:
  *   CAST(1, 'Bool') IN (1)   -> 1  (1 is representable as Bool, matches true)
  *   CAST(1, 'Bool') IN (255) -> 0  (255 is not representable, excluded from set)
  *   CAST(1, 'Bool') IN (256) -> 0  (256 is not representable, excluded from set)
  *   CAST('33.3', 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)'))           -> 0
  *   CAST('33.3', 'Decimal64(1)') IN (33.33)                                   -> 0
  *   (CAST('33.3', 'Decimal64(1)'), 1) IN ((CAST('33.33', 'Decimal64(2)'), 1)) -> 0
  */
Field convertFieldToType(
    const Field & from_value,
    const IDataType & to_type,
    const IDataType * from_type_hint = nullptr,
    const FormatSettings & format_settings = {},
    bool strict = false);

/// Same as convertFieldToType but returns empty Field in case of an exception.
Field tryConvertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint = nullptr, const FormatSettings & format_settings = {});

/// Does the same, but throws ARGUMENT_OUT_OF_BOUND if value does not fall into the range.
Field convertFieldToTypeOrThrow(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint = nullptr, const FormatSettings & format_settings = {});

}
