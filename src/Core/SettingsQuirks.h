#pragma once

#include <Common/Logger.h>

namespace DB
{

struct Settings;

/// Update some settings defaults to avoid some known issues.
void applySettingsQuirks(Settings & settings, LoggerPtr log = nullptr);

/// Verify that some settings have sane values. Throws if not
void doSettingsSanityCheck(const Settings & settings);
}
