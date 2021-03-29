#pragma once
#include <cstddef>
#include <Interpreters/ExpressionActionsSettings.h>

namespace DB
{

struct Settings;
class Context;

struct BuildQueryPipelineSettings
{
    ExpressionActionsSettings actions_settings;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }

    static BuildQueryPipelineSettings fromSettings(const Settings & from);
    static BuildQueryPipelineSettings fromContext(const Context & from);
};

}
