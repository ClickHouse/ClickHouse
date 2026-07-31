#pragma once

#include <Core/Block.h>
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
using BlockMarshallingCallback = std::function<Block(const Block & block)>;
struct IParameterLookup;
using ParameterLookupPtr = std::shared_ptr<IParameterLookup>;
struct IExchangeLookup;
using ExchangeLookupPtr = std::shared_ptr<IExchangeLookup>;
class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;

struct BuildQueryPipelineSettings
{
    explicit BuildQueryPipelineSettings(ContextPtr from);

    bool enable_multiple_filters_transforms_for_and_chain;

    ExpressionActionsSettings actions_settings;
    QueryStatusPtr process_list_element;
    ProgressCallback progress_callback;
    TemporaryFileLookupPtr temporary_file_lookup;
    BlockMarshallingCallback block_marshalling_callback;
    ParameterLookupPtr parameter_lookup;
    ExchangeLookupPtr exchange_lookup;

    /// Temporary data scope of the query that is being executed (per-query scope installed by `ProcessList::insert`,
    /// or the server-wide root when there is none). Steps that spill to disk must create their child scopes from it,
    /// otherwise `max_temporary_data_on_disk_size_for_query` / `..._for_user` are not accounted.
    TemporaryDataOnDiskScopePtr tmp_data_scope;


    size_t max_threads;
    size_t aggregation_memory_efficient_merge_threads;
    size_t min_outstreams_per_resize_after_split;
    size_t max_streams_for_union_step;
    double max_streams_for_union_step_to_max_threads_ratio;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }
};

}
