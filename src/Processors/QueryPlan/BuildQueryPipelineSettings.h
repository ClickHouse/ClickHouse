#pragma once

#include <IO/Progress.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <cstddef>


namespace DB
{

struct Settings;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
struct ITemporaryFileLookup;
using TemporaryFileLookupPtr = std::shared_ptr<ITemporaryFileLookup>;

struct BuildQueryPipelineSettings
{
    bool enable_multiple_filters_transforms_for_and_chain = true;

    ExpressionActionsSettings actions_settings;
    QueryStatusPtr process_list_element;
    ProgressCallback progress_callback = nullptr;
    TemporaryFileLookupPtr temporary_file_lookup;

    size_t max_threads;
    size_t aggregation_memory_efficient_merge_threads;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }
    static BuildQueryPipelineSettings fromContext(ContextPtr from);
};

}
