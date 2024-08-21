#pragma once

namespace Poco
{
class Logger;
}

namespace DB
{

struct Settings;

/// Update some settings defaults to avoid some known issues.
void applySettingsQuirks(Settings & settings, Poco::Logger * log = nullptr);

}
