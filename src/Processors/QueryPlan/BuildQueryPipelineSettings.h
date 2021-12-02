#pragma once

#include <Interpreters/ExpressionActionsSettings.h>

#include <cstddef>

namespace DB
{

struct Settings;
class QueryStatus;

struct BuildQueryPipelineSettings
{
    ExpressionActionsSettings actions_settings;
    QueryStatus * process_list_element = nullptr;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }
    static BuildQueryPipelineSettings fromContext(ContextPtr from);
};

}
