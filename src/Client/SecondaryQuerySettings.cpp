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
            bool last_profile_traces_value = false;
            for (const auto & change : set_query->changes)
            {
                /// Validate every occurrence, including values overridden by a later duplicate.
                if (change.name == "send_profile_traces")
                    last_profile_traces_value = SettingFieldBool(change.value).value;
            }

            std::erase_if(set_query->changes, [last_profile_traces_value](const SettingChange & change)
            {
                /// A final opt-in must also remove earlier opt-outs from the same clause.
                return change.name == "send_profile_traces" && (last_profile_traces_value || SettingFieldBool(change.value).value);
            });
        }
        for (const auto & child : node->children)
            if (child)
                nodes.push_back(child.get());
    }
}

}
