#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Pipe.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
//#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace DB
{

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

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

    /// Part of settings which are needed for reading.
    struct Settings
    {
        UInt64 max_block_size;
        size_t num_streams;
        size_t preferred_block_size_bytes;
        size_t preferred_max_column_in_block_size_bytes;
        //size_t min_marks_for_concurrent_read;
        bool use_uncompressed_cache;
        bool force_primary_key;
        bool sample_factor_column_queried;

        MergeTreeReaderSettings reader_settings;
        MergeTreeReadPool::BackoffSettings backoff_settings;
    };

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
        const SelectQueryInfo & query_info_,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
        ContextPtr context_,
        const MergeTreeData & data_,
        StorageMetadataPtr metadata_snapshot_,
        StorageMetadataPtr metadata_snapshot_base_,
        Names real_column_names_,
        MergeTreeData::DataPartsVector parts_,
        PrewhereInfoPtr prewhere_info_,
        Names virt_column_names_,
        Settings settings_,
        Poco::Logger * log_
    );

    String getName() const override { return "ReadFromMergeTree"; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeIndexes(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeIndexes(JSONBuilder::JSONMap & map) const override;

private:
    SelectQueryInfo query_info;
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read;
    ContextPtr context;
    const MergeTreeData & data;
    StorageMetadataPtr metadata_snapshot;
    StorageMetadataPtr metadata_snapshot_base;

    Names real_column_names;
    MergeTreeData::DataPartsVector prepared_parts;
    PrewhereInfoPtr prewhere_info;
    Names virt_column_names;
    Settings settings;

    Poco::Logger * log;

    Pipe read(RangesInDataParts parts_with_range, Names required_columns, ReadType read_type, size_t used_max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readFromPool(RangesInDataParts parts_with_ranges, Names required_columns, size_t used_max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
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
