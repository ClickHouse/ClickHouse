#pragma once

#include <Common/Logger.h>

namespace DB
{

struct Settings;

/// Update some settings defaults to avoid some known issues.
void applySettingsQuirks(Settings & settings, LoggerPtr log = nullptr);

/// Verify that some settings have sane values. Alters the value to a reasonable one if not
void doSettingsSanityCheckClamp(Settings & settings, LoggerPtr log);
}
