#pragma once

#include <Interpreters/ExpressionActionsSettings.h>

#include <cstddef>

namespace DB
{

struct Settings;

struct BuildQueryPipelineSettings
{
    ExpressionActionsSettings actions_settings;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }

    static BuildQueryPipelineSettings fromSettings(const Settings & from);
    static BuildQueryPipelineSettings fromContext(ContextPtr from);
};

}
