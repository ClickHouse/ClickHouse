#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/KeyDescription.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class ReadFromCountByGranularity final : public ISourceStep
{
public:
    ReadFromCountByGranularity(
        SharedHeader output_header_,
        RangesInDataParts parts_with_ranges_,
        ExpressionActionsPtr bucket_expression_,
        Names group_by_key_names_,
        KeyDescription primary_key_,
        AggregateFunctionPtr count_function_,
        StorageSnapshotPtr storage_snapshot_,
        MergeTreeSettingsPtr data_settings_,
        ContextPtr context_,
        size_t num_streams_,
        ExpressionActionsPtr filter_expression_,
        String filter_column_name_,
        bool has_filter_);

    String getName() const override { return "ReadFromCountByGranularity"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    Pipe makePipe();

    RangesInDataParts parts_with_ranges;
    ExpressionActionsPtr bucket_expression;
    Names group_by_key_names;
    KeyDescription primary_key;
    AggregateFunctionPtr count_function;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeSettingsPtr data_settings;
    ContextPtr context;
    size_t num_streams;
    ExpressionActionsPtr filter_expression;
    String filter_column_name;
    bool has_filter;
};

}
