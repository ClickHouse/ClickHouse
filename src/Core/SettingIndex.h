#pragma once

#include <cstddef>

/// Typed index for accessing a setting field within a settings Data struct.
/// Stores the byte offset of the field, enabling type-safe access via operator[].
/// The Owner tag prevents cross-class index misuse at compile time:
/// e.g. SettingIndex<Settings, SettingFieldBool> is a different type from
/// SettingIndex<MergeTreeSettings, SettingFieldBool>.
/// Defined outside namespace DB so it can be included from any scope.
template <typename Owner, typename FieldType>
struct SettingIndex
{
    size_t offset;
};
