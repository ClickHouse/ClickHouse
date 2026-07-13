#pragma once

#include <cstdint>

enum class SettingsWriteFormat : uint8_t
{
    BINARY = 0, /// Part of the settings are serialized as strings, and other part as variants. This is the old behaviour.
    STRINGS_WITH_FLAGS = 1, /// All settings are serialized as strings. Before each value the flag `is_important` is serialized.
    DEFAULT = STRINGS_WITH_FLAGS,
};
