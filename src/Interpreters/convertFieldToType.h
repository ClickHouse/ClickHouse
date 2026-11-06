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
  * When `strict` is false (default), Bool conversion clamps any non-zero integer to 1, and Decimal
  * conversions may silently round/truncate:
  *   convertFieldToType(Field(255), Bool)                 -> Field(true)   i.e. 1
  *   convertFieldToType(Field(256), Bool)                 -> Field(true)   i.e. 1
  *   convertFieldToType(Field(1),   Bool)                 -> Field(true)   i.e. 1
  *   convertFieldToType(Decimal64("33.33"), Decimal64(1)) -> Decimal64("33.3")  (truncated)
  *
  * Conversion to a floating-point type, however, stays exact by default: a value that is not exactly
  * representable in the target type returns Null, e.g. convertFieldToType(Field(0.1), Float32) -> Null.
  * This is what optimizer/pruning callers (`KeyCondition`, sharding-key rewrite, ...) rely on: rounding
  * the constant and then treating it as an exact comparison bound could prune a mark or a shard that
  * actually contains a matching row.
  *
  * `convert_inexact_floats` opts into lossy floating-point conversion: a value that is not exactly
  * representable is converted to the nearest representable value, like CAST. It is meant for paths that
  * materialize a value rather than build a comparison bound (the `values`/`VALUES` table function, the
  * `INSERT` VALUES section, `WITH FILL`, window frame offsets, ...). These callers pass
  * `convert_inexact_floats = true` explicitly (through `convertFieldToTypeOrThrow`, or directly for the
  * `INSERT` VALUES expression fallback in `ValuesBlockInputFormat`), while callers that resolve an exact
  * target - notably `ALTER ... PARTITION` resolution in `MergeTreeData::getPartitionIDFromQuery` - keep
  * the exact default, so a destructive statement never silently rounds an unrepresentable numeric literal
  * (a quoted string literal such as `'0.1'` is still parsed into the target type by string deserialization
  * first, which rounds - a pre-existing behavior shared with string-to-float value comparisons like
  * `WHERE f = '0.1'`, unchanged by this contract):
  *   convertFieldToType(Field(0.1), Float32, .., false, true) -> Field(0.1f)  (nearest Float32, not exactly 0.1)
  *   convertFieldToType(Field(0.5), Float32, .., false, true) -> Field(0.5f)  (exactly representable)
  *
  * When `strict` is true, conversions that lose precision return Null instead (`convert_inexact_floats`
  * is ignored - strict never rounds):
  *   - Bool: only 0 and 1 are representable; other values produce Null:
  *       convertFieldToType(Field(255), Bool, .., true) -> Field(Null)
  *       convertFieldToType(Field(256), Bool, .., true) -> Field(Null)
  *       convertFieldToType(Field(1),   Bool, .., true) -> Field(true)  i.e. 1
  *   - Float -> Float (narrowing): rejects values not exactly representable in the target type:
  *       convertFieldToType(Field(0.1), Float32, .., true) -> Field(Null)
  *       convertFieldToType(Field(0.5), Float32, .., true) -> Field(0.5f)
  *   - Decimal -> Decimal: rejects any lossy conversion by requiring exact equality after conversion.
  *   - Float64 -> Decimal: converts the Decimal back to Float64 and compares with the original.
  *
  * Out-of-range values are always rejected (return Null) regardless of `strict`/`convert_inexact_floats`,
  * e.g. convertFieldToType(Field(1e300), Float32, .., false, true) -> Field(Null) (no silent overflow to inf).
  *
  * The strictness checks apply recursively inside composite types (Tuple, Array, Map), so e.g. a
  * Tuple(Decimal64(2)) element inside an Array is also checked for precision loss, and a Float32 element
  * is rounded only when `convert_inexact_floats` is set.
  *
  * This makes the IN operator use exact value semantics:
  *   CAST(1, 'Bool') IN (1)   -> 1  (1 is representable as Bool, matches true)
  *   CAST(1, 'Bool') IN (255) -> 0  (255 is not representable, excluded from set)
  *   CAST(1, 'Bool') IN (256) -> 0  (256 is not representable, excluded from set)
  *   toFloat32(0.1) IN (0.1)  -> 0  (0.1 is not representable as Float32, just like toFloat32(0.1) = 0.1)
  *   toFloat32(0.5) IN (0.5)  -> 1  (0.5 is representable as Float32)
  *   CAST('33.3', 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)'))           -> 0
  *   CAST('33.3', 'Decimal64(1)') IN (33.33)                                   -> 0
  *   (CAST('33.3', 'Decimal64(1)'), 1) IN ((CAST('33.33', 'Decimal64(2)'), 1)) -> 0
  */
Field convertFieldToType(
    const Field & from_value,
    const IDataType & to_type,
    const IDataType * from_type_hint = nullptr,
    const FormatSettings & format_settings = {},
    bool strict = false,
    bool convert_inexact_floats = false);

/// Same as convertFieldToType but returns empty Field in case of an exception.
Field tryConvertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint = nullptr, const FormatSettings & format_settings = {}, bool strict = false, bool convert_inexact_floats = false);

/// Does the same, but throws ARGUMENT_OUT_OF_BOUND if value does not fall into the range.
/// `convert_inexact_floats` is off by default so that callers resolving an exact target (e.g.
/// `ALTER ... PARTITION` resolution) reject float literals that are not exactly representable instead
/// of silently rounding them. Value-materialization callers (the `values`/`VALUES` table function,
/// `WITH FILL`, window frame offsets, ...) pass it as true to convert to the nearest representable
/// floating-point value like CAST.
Field convertFieldToTypeOrThrow(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint = nullptr, const FormatSettings & format_settings = {}, bool convert_inexact_floats = false);

}
