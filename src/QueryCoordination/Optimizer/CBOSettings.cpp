#include "CBOSettings.h"
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

CBOSettings CBOSettings::fromSettings(const Settings & from)
{
    CBOSettings settings;
    settings.cbo_aggregating_mode = from.cbo_aggregating_mode;
    settings.cbo_topn_mode = from.cbo_topn_mode;
    settings.cbo_sorting_mode = from.cbo_sorting_mode;
    settings.cbo_limiting_mode = from.cbo_limiting_mode;
    return settings;
}

CBOSettings CBOSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}
