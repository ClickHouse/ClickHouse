#pragma once

namespace DB
{

struct Settings;

/// Update some settings defaults for clickhouse-local
void applySettingsOverridesForLocal(Settings & settings);

}
