#pragma once

#include <Core/SettingIndex.h>
#include <Core/SettingOrigin.h>

#include <string_view>
#include <type_traits>
#include <utility>

/// Forward-declares CLASS_NAME so it can be used as a template tag in SettingIndex,
/// even though the full struct definition comes later in the header.
/// The repeated forward declaration is harmless in C++ and avoids requiring
/// each header to manually add one before calling SUPPORTED_TYPES.
#define DECLARE_SETTING_TRAIT(CLASS_NAME, TYPE) \
    struct CLASS_NAME; \
    using CLASS_NAME##TYPE = SettingIndex<CLASS_NAME, SettingField##TYPE>;

#define DECLARE_SETTING_SUBSCRIPT_OPERATOR(CLASS_NAME, TYPE) \
    const SettingField##TYPE & operator[](CLASS_NAME##TYPE t) const; \
    SettingField##TYPE & operator[](CLASS_NAME##TYPE t);

/// The typed `set` of a public settings class whose traits record origins (`DECLARE_SETTINGS_TRAITS_WITH_ORIGIN`),
/// and what it needs from the `Impl` it holds behind an incomplete type; defined by `IMPLEMENT_SETTINGS_TYPED_SET`.
/// `set` assigns `value` and records `origin`, unlike an assignment through `operator[]`: see `BaseSettings`.
/// `nameAtOffset` names the setting a typed index points at, for matching a row of a described vector.
#define DECLARE_SETTINGS_TYPED_SET(CLASS_NAME) \
    template <typename FieldType, typename Value> \
    void set(SettingIndex<CLASS_NAME, FieldType> setting, Value && value, SettingOrigin origin = SettingOrigin::Default) \
    { \
        /* A whole setting field would be copied with its `changed` flag, and a `false` one would hide the origin */ \
        /* recorded below: enumeration reads it only for a setting that counts as changed. */ \
        static_assert(!requires(const std::remove_cvref_t<Value> & field) { field.changed; }, \
                      "`set` takes a value, not a setting field"); \
        (*this)[setting] = std::forward<Value>(value); \
        recordOriginAtOffset(setting.offset, origin); \
    } \
    void recordOriginAtOffset(size_t offset, SettingOrigin origin); \
    static std::string_view nameAtOffset(size_t offset);
