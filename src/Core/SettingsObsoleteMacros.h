#pragma once

// clang-format off
#define MAKE_OBSOLETE(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "Obsolete setting, does nothing.", BaseSettingsHelpers::Flags::OBSOLETE)

/// NOTE: ServerSettings::loadSettingsFromConfig() should be updated to include this settings
#define MAKE_DEPRECATED_BY_SERVER_CONFIG(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "User-level setting is deprecated, and it must be defined in the server configuration instead.", BaseSettingsHelpers::Flags::OBSOLETE)
