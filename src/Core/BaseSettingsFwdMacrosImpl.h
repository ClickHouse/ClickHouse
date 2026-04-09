#pragma once

#define IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR(CLASS_NAME, TYPE) \
    const SettingField##TYPE & CLASS_NAME::operator[](CLASS_NAME##TYPE t) const \
    { \
        return *reinterpret_cast<const SettingField##TYPE *>( \
            reinterpret_cast<const char *>(impl.get()) + t.offset); \
    } \
    SettingField##TYPE & CLASS_NAME::operator[](CLASS_NAME##TYPE t) \
    { \
        return *reinterpret_cast<SettingField##TYPE *>( \
            reinterpret_cast<char *>(impl.get()) + t.offset); \
    }
