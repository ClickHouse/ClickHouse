#pragma once

#include <cstddef>
#include <string_view>

/// Typed index for accessing a setting field within a settings Data struct.
/// Stores the byte offset of the field, enabling type-safe access via operator[].
/// The Owner tag prevents cross-class index misuse at compile time:
/// e.g. SettingIndex<Settings, SettingFieldBool> is a different type from
/// SettingIndex<MergeTreeSettings, SettingFieldBool>.
/// Defined outside namespace DB so it can be included from any scope.
///
/// The default `offset` is the all-ones sentinel rather than `0`, so a SettingIndex
/// that escapes brace-initialization crashes on use (out-of-bounds address) instead
/// of silently aliasing the first field at offset 0.
///
/// `name` is the setting's declared name, which the macro that defines each index has as a token
/// anyway. Code that has to name a setting - one that matches rows of a settings table by name, say -
/// can then take the index rather than a string literal, so a misspelled or renamed setting does not
/// compile instead of silently matching nothing.
template <typename Owner, typename FieldType>
struct SettingIndex
{
    size_t offset = static_cast<size_t>(-1);
    std::string_view name = {};
};
