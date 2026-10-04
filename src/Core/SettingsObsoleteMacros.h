#pragma once

// clang-format off
#define MAKE_OBSOLETE(M, TYPE, NAME, DEFAULT, ...) \
    M(TYPE, NAME, DEFAULT, "Obsolete setting, does nothing.", SettingsTierType::OBSOLETE __VA_OPT__(,) __VA_ARGS__)

/// NOTE: ServerSettings::loadSettingsFromConfig() should be updated to include this settings
#define MAKE_DEPRECATED_BY_SERVER_CONFIG(M, TYPE, NAME, DEFAULT, ...) \
    M(TYPE, NAME, DEFAULT, "User-level setting is deprecated, and it must be defined in the server configuration instead.", SettingsTierType::OBSOLETE __VA_OPT__(,) __VA_ARGS__)
