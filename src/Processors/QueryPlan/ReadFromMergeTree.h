#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

class Pipe;

/// This step is created to read from MergeTree* table.
/// For now, it takes a list of parts and creates source from it.
class ReadFromMergeTree final : public ISourceStep
{
public:

    enum class IndexType
    {
        None,
        MinMax,
        Partition,
        PrimaryKey,
        Skip,
    };

    /// This is a struct with information about applied indexes.
    /// Is used for introspection only, in EXPLAIN query.
    struct IndexStat
    {
        IndexType type;
        std::string name;
        std::string description;
        std::string condition;
        std::vector<std::string> used_keys;
        size_t num_parts_after;
        size_t num_granules_after;
    };

    using IndexStats = std::vector<IndexStat>;

    enum class ReadType
    {
        /// By default, read will use MergeTreeReadPool and return pipe with num_streams outputs.
        /// If num_streams == 1, will read without pool, in order specified in parts.
        Default,
        /// Read in sorting key order.
        /// Returned pipe will have the number of ports equals to parts.size().
        /// Parameter num_streams_ is ignored in this case.
        /// User should add MergingSorted itself if needed.
        InOrder,
        /// The same as InOrder, but in reverse order.
        /// For every part, read ranges and granules from end to begin. Also add ReverseTransform.
        InReverseOrder,
    };

    ReadFromMergeTree(
        MergeTreeData::DataPartsVector parts_,
        Names real_column_names_,
        Names virt_column_names_,
        const MergeTreeData & data_,
        const SelectQueryInfo & query_info_,
        StorageMetadataPtr metadata_snapshot_,
        StorageMetadataPtr metadata_snapshot_base_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_,
        bool sample_factor_column_queried_,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
        Poco::Logger * log_
    );

    String getName() const override { return "ReadFromMergeTree"; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeIndexes(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeIndexes(JSONBuilder::JSONMap & map) const override;

private:
    const MergeTreeReaderSettings reader_settings;

    MergeTreeData::DataPartsVector prepared_parts;
    Names real_column_names;
    Names virt_column_names;

    const MergeTreeData & data;
    SelectQueryInfo query_info;
    PrewhereInfoPtr prewhere_info;
    ExpressionActionsSettings actions_settings;

    StorageMetadataPtr metadata_snapshot;
    StorageMetadataPtr metadata_snapshot_base;

    ContextPtr context;

    const size_t max_block_size;
    const size_t requested_num_streams;
    const size_t preferred_block_size_bytes;
    const size_t preferred_max_column_in_block_size_bytes;
    const bool sample_factor_column_queried;

    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read;

    Poco::Logger * log;

    Pipe read(RangesInDataParts parts_with_range, Names required_columns, ReadType read_type, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readFromPool(RangesInDataParts parts_with_ranges, Names required_columns, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readInOrder(RangesInDataParts parts_with_range, Names required_columns, ReadType read_type, bool use_uncompressed_cache);

    template<typename TSource>
    ProcessorPtr createSource(const RangesInDataPart & part, const Names & required_columns, bool use_uncompressed_cache);

    Pipe spreadMarkRangesAmongStreams(
        RangesInDataParts && parts_with_ranges,
        const Names & column_names);

    Pipe spreadMarkRangesAmongStreamsWithOrder(
        RangesInDataParts && parts_with_ranges,
        const Names & column_names,
        const ActionsDAGPtr & sorting_key_prefix_expr,
        ActionsDAGPtr & out_projection,
        const InputOrderInfoPtr & input_order_info);

    Pipe spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts,
        const Names & column_names,
        ActionsDAGPtr & out_projection);

    struct AnalysisResult;
    AnalysisResult selectRangesToRead(MergeTreeData::DataPartsVector parts) const;
};

}
