#pragma once

#include <Common/Logger.h>

namespace DB
{

struct Settings;

/// Update some settings defaults to avoid some known issues.
void applySettingsQuirks(Settings & settings, LoggerPtr log = nullptr);

}
