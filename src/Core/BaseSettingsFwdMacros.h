#pragma once

#include <Core/SettingIndex.h>

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
