#pragma once

#include <Core/Field.h>


namespace DB
{

/// Conversions of a value from a query or a config to the type of a setting. They are shared by the setting
/// fields in `SettingsFields.h` and by code which reads a setting from an AST without using the setting classes.

/// Accepts "0", "1", "false" and "true" (case-insensitive).
bool stringToBoolSettingValue(const String & str);

/// Parses a number with an optional size suffix (e.g. "10K"), or a bool if T is bool.
template <typename T>
T stringToNumberSettingValue(const String & str);

/// Converts a numeric, bool or string field, checking that the value fits in T.
template <typename T>
T fieldToNumberSettingValue(const Field & f);

/// Throws if a floating point value isn't finite. Does nothing for other types.
template <typename T>
void validateFloatingPointSettingValue(T value);

}
