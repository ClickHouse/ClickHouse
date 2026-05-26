#pragma once

/// `t.offset` is relative to `Traits::Data`; delegate to `BaseSettings::operator[]` which
/// performs the derived-to-base conversion on a real Impl pointer (rather than reinterpreting
/// raw bytes from `impl.get()` directly, which would skip the Impl->Data subobject offset).
#define IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR(CLASS_NAME, TYPE) \
    const SettingField##TYPE & CLASS_NAME::operator[](CLASS_NAME##TYPE t) const \
    { \
        return (*impl)[t]; \
    } \
    SettingField##TYPE & CLASS_NAME::operator[](CLASS_NAME##TYPE t) \
    { \
        return (*impl)[t]; \
    }
