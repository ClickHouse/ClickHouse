#include <Client/SecondaryQuerySettings.h>

#include <Common/CurrentThread.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTSetQuery.h>

#include <algorithm>
#include <vector>

namespace DB
{

namespace Setting
{
    extern const SettingsDialect dialect;
    extern const SettingsBool send_profile_traces;
}

void prepareSecondaryQuerySettings(Settings & settings)
{
    settings.markSettingsChangedByCompatibilityAsUnchanged();
    settings[Setting::dialect] = Dialect::clickhouse;
    if (!CurrentThread::getInternalProfileTracesQueue())
        settings[Setting::send_profile_traces] = false;
}

void stripProfileTraceOptInsFromQuery(const ASTPtr & query)
{
    if (!query)
        return;

    std::vector<IAST *> nodes{query.get()};
    while (!nodes.empty())
    {
        auto * node = nodes.back();
        nodes.pop_back();
        if (auto * set_query = node->as<ASTSetQuery>())
        {
            std::erase_if(set_query->changes, [](const SettingChange & change)
            {
                return change.name == "send_profile_traces" && SettingFieldBool(change.value).value;
            });
        }
        for (const auto & child : node->children)
            if (child)
                nodes.push_back(child.get());
    }
}

}
