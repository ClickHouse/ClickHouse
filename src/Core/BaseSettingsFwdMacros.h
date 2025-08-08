#pragma once

#define DECLARE_SETTING_TRAIT(CLASS_NAME, TYPE) using CLASS_NAME##TYPE = SettingField##TYPE CLASS_NAME##Impl::*;

#define DECLARE_SETTING_SUBSCRIPT_OPERATOR(CLASS_NAME, TYPE) \
    const SettingField##TYPE & operator[](CLASS_NAME##TYPE t) const; \
    SettingField##TYPE & operator[](CLASS_NAME##TYPE t);
