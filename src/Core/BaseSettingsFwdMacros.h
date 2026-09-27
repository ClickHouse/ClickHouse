#pragma once

#include <Core/SettingIndex.h>
#include <Core/SettingOrigin.h>

#include <string_view>
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
/// and what it needs from the `Impl` the class holds behind an incomplete type. Defined in the .cpp by
/// `IMPLEMENT_SETTINGS_TYPED_SET`.
///
/// `set` assigns `value` and records `origin` as where it came from - unlike an assignment through `operator[]`,
/// which keeps whatever origin was recorded, and so suits only a value adjusted in place, such as by expanding
/// macros. By the setting's typed index, so that a misspelled or renamed setting does not compile, and with the
/// value in whatever form the setting's field accepts - an atomic or an enum member of a storage included.
/// `nameAtOffset` names the setting a typed index points at, for matching a row of a described vector.
#define DECLARE_SETTINGS_TYPED_SET(CLASS_NAME) \
    template <typename FieldType, typename Value> \
    void set(SettingIndex<CLASS_NAME, FieldType> setting, Value && value, SettingOrigin origin = SettingOrigin::Default) \
    { \
        (*this)[setting] = std::forward<Value>(value); \
        recordOriginAtOffset(setting.offset, origin); \
    } \
    void recordOriginAtOffset(size_t offset, SettingOrigin origin); \
    static std::string_view nameAtOffset(size_t offset);
