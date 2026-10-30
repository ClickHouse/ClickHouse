#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

/// Source step created by the `optimizeTrivialCountFromTextIndex` pass.
/// Reads the text index at execution time and emits one `count()` aggregate state per part,
/// which the `AggregatingStep` above merges into the final count. Parts are processed
/// in parallel by `num_streams` sources, and the reads are cancellable between parts.
class ReadFromTextIndexCount : public ISourceStep
{
public:
    /// The text search query recovered from the index read tasks at plan time.
    struct ResolvedQuery
    {
        MergeTreeIndexWithCondition index;
        std::shared_ptr<MergeTreeIndexConditionText> condition;
        TextSearchQueryPtr query;
    };

    /// `replaced_reading` is the read this step replaces entirely, its index usage is reported when the
    /// pipeline is built; nullptr when that read stays in the plan and reports it itself.
    ReadFromTextIndexCount(
        RangesInDataParts parts_,
        ResolvedQuery resolved_,
        MergeTreeReaderSettings reader_settings_,
        const String & count_column_name,
        size_t num_streams_,
        const ReadFromMergeTree * replaced_reading);

    String getName() const override { return "ReadFromTextIndexCount"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    RangesInDataParts parts;
    std::shared_ptr<const ResolvedQuery> resolved;
    MergeTreeReaderSettings reader_settings;
    size_t num_streams;

    ReadFromMergeTree::AnalysisResultPtr replaced_analysis;
    StorageID storage_id = StorageID::createEmpty();
    ContextMutablePtr access_info_context;
};

}
