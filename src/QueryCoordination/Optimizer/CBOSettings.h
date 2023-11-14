#pragma once

#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

struct Settings;

struct CBOSettings
{
    /// Provide a method with which user can specify aggregating execution mode.
    /// Default is calculated by CBO optimizer.
    CBOStepExecutionMode cbo_aggregating_mode;

    /// Just like 'cbo_aggregating_mode', user can specify topn execution mode.
    /// Default is calculated by CBO optimizer.
    CBOStepExecutionMode cbo_topn_mode;

    /// Just like 'cbo_aggregating_mode', user can specify sorting execution mode.
    /// Default is calculated by CBO optimizer.
    CBOStepExecutionMode cbo_sorting_mode;

    /// Just like 'cbo_aggregating_mode', user can specify limiting execution mode.
    /// Default is calculated by CBO optimizer.
    CBOStepExecutionMode cbo_limiting_mode;

    static CBOSettings fromSettings(const Settings & from);
    static CBOSettings fromContext(ContextPtr from);
};

}
