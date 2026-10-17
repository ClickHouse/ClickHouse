#pragma once

#define IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR(CLASS_NAME, TYPE) \
    const SettingField##TYPE & CLASS_NAME::operator[](CLASS_NAME##TYPE t) const \
    { \
        return impl.get()->*t; \
    } \
    SettingField##TYPE & CLASS_NAME::operator[](CLASS_NAME##TYPE t) \
    { \
        return impl.get()->*t; \
    }
