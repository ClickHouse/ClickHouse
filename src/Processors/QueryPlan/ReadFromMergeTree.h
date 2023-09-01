#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

class Pipe;

using MergeTreeReadTaskCallback = std::function<std::optional<PartitionReadResponse>(PartitionReadRequest)>;

struct MergeTreeDataSelectSamplingData
{
    bool use_sampling = false;
    bool read_nothing = false;
    Float64 used_sample_factor = 1.0;
    std::shared_ptr<ASTFunction> filter_function;
    ActionsDAGPtr filter_expression;
};

struct MergeTreeDataSelectAnalysisResult;
using MergeTreeDataSelectAnalysisResultPtr = std::shared_ptr<MergeTreeDataSelectAnalysisResult>;

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

    struct AnalysisResult
    {
        RangesInDataParts parts_with_ranges;
        MergeTreeDataSelectSamplingData sampling;
        IndexStats index_stats;
        Names column_names_to_read;
        ReadFromMergeTree::ReadType read_type = ReadFromMergeTree::ReadType::Default;
        UInt64 total_parts = 0;
        UInt64 parts_before_pk = 0;
        UInt64 selected_parts = 0;
        UInt64 selected_ranges = 0;
        UInt64 selected_marks = 0;
        UInt64 selected_marks_pk = 0;
        UInt64 total_marks_pk = 0;
        UInt64 selected_rows = 0;
    };

    ReadFromMergeTree(
        MergeTreeData::DataPartsVector parts_,
        Names real_column_names_,
        Names virt_column_names_,
        const MergeTreeData & data_,
        const SelectQueryInfo & query_info_,
        StorageSnapshotPtr storage_snapshot,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_,
        bool sample_factor_column_queried_,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
        Poco::Logger * log_,
        MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr_,
        bool enable_parallel_reading
    );

    static constexpr auto name = "ReadFromMergeTree";
    String getName() const override { return name; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeIndexes(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeIndexes(JSONBuilder::JSONMap & map) const override;

    void addFilter(ActionsDAGPtr expression, std::string column_name)
    {
        added_filter_dags.push_back(expression);
        added_filter_nodes.nodes.push_back(&expression->findInOutputs(column_name));
    }

    void addFilterNodes(const ActionDAGNodes & filter_nodes)
    {
        for (const auto & node : filter_nodes.nodes)
            added_filter_nodes.nodes.push_back(node);
    }

    StorageID getStorageID() const { return data.getStorageID(); }
    UInt64 getSelectedParts() const { return selected_parts; }
    UInt64 getSelectedRows() const { return selected_rows; }
    UInt64 getSelectedMarks() const { return selected_marks; }

    static MergeTreeDataSelectAnalysisResultPtr selectRangesToRead(
        MergeTreeData::DataPartsVector parts,
        const PrewhereInfoPtr & prewhere_info,
        const ActionDAGNodes & added_filter_nodes,
        const StorageMetadataPtr & metadata_snapshot_base,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        unsigned num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read,
        const MergeTreeData & data,
        const Names & real_column_names,
        bool sample_factor_column_queried,
        Poco::Logger * log);

    ContextPtr getContext() const { return context; }
    const SelectQueryInfo & getQueryInfo() const { return query_info; }
    StorageMetadataPtr getStorageMetadata() const { return metadata_for_reading; }

    void setQueryInfoOrderOptimizer(std::shared_ptr<ReadInOrderOptimizer> read_in_order_optimizer);
    void setQueryInfoInputOrderInfo(InputOrderInfoPtr order_info);

private:
    int getSortDirection() const
    {
        const InputOrderInfoPtr & order_info = query_info.getInputOrderInfo();
        if (order_info)
            return order_info->direction;

        return 1;
    }

    const MergeTreeReaderSettings reader_settings;

    MergeTreeData::DataPartsVector prepared_parts;
    Names real_column_names;
    Names virt_column_names;

    const MergeTreeData & data;
    SelectQueryInfo query_info;
    PrewhereInfoPtr prewhere_info;
    ExpressionActionsSettings actions_settings;

    std::vector<ActionsDAGPtr> added_filter_dags;
    ActionDAGNodes added_filter_nodes;

    StorageSnapshotPtr storage_snapshot;
    StorageMetadataPtr metadata_for_reading;

    ContextPtr context;

    const size_t max_block_size;
    const size_t requested_num_streams;
    const size_t preferred_block_size_bytes;
    const size_t preferred_max_column_in_block_size_bytes;
    const bool sample_factor_column_queried;

    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read;

    Poco::Logger * log;
    UInt64 selected_parts = 0;
    UInt64 selected_rows = 0;
    UInt64 selected_marks = 0;

    Pipe read(RangesInDataParts parts_with_range, Names required_columns, ReadType read_type, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readFromPool(RangesInDataParts parts_with_ranges, Names required_columns, size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache);
    Pipe readInOrder(RangesInDataParts parts_with_range, Names required_columns, ReadType read_type, bool use_uncompressed_cache, UInt64 limit);

    template<typename TSource>
    ProcessorPtr createSource(const RangesInDataPart & part, const Names & required_columns, bool use_uncompressed_cache, bool has_limit_below_one_block);

    Pipe spreadMarkRangesAmongStreams(
        RangesInDataParts && parts_with_ranges,
        const Names & column_names);

    Pipe spreadMarkRangesAmongStreamsWithOrder(
        RangesInDataParts && parts_with_ranges,
        const Names & column_names,
        ActionsDAGPtr & out_projection,
        const InputOrderInfoPtr & input_order_info);

    Pipe spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts,
        const Names & column_names,
        ActionsDAGPtr & out_projection);

    MergeTreeDataSelectAnalysisResultPtr selectRangesToRead(MergeTreeData::DataPartsVector parts) const;
    ReadFromMergeTree::AnalysisResult getAnalysisResult() const;
    MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr;

    std::optional<MergeTreeReadTaskCallback> read_task_callback;
};

struct MergeTreeDataSelectAnalysisResult
{
    std::variant<std::exception_ptr, ReadFromMergeTree::AnalysisResult> result;

    bool error() const;
    size_t marks() const;
};

}
