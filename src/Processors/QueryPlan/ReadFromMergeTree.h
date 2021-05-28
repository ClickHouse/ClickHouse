#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Pipe.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>

namespace DB
{

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
    using IndexStatPtr = std::unique_ptr<IndexStats>;

    /// Part of settings which are needed for reading.
    struct Settings
    {
        UInt64 max_block_size;
        size_t preferred_block_size_bytes;
        size_t preferred_max_column_in_block_size_bytes;
        size_t min_marks_for_concurrent_read;
        bool use_uncompressed_cache;

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
        const MergeTreeData & storage_,
        StorageMetadataPtr metadata_snapshot_,
        String query_id_,
        Names required_columns_,
        RangesInDataParts parts_,
        IndexStatPtr index_stats_,
        PrewhereInfoPtr prewhere_info_,
        Names virt_column_names_,
        Settings settings_,
        size_t num_streams_,
        ReadType read_type_
    );

    String getName() const override { return "ReadFromMergeTree"; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeIndexes(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeIndexes(JSONBuilder::JSONMap & map) const override;

private:
    const MergeTreeData & storage;
    StorageMetadataPtr metadata_snapshot;
    String query_id;

    Names required_columns;
    RangesInDataParts parts;
    IndexStatPtr index_stats;
    PrewhereInfoPtr prewhere_info;
    Names virt_column_names;
    Settings settings;

    size_t num_streams;
    ReadType read_type;

    Pipe read();
    Pipe readFromPool();
    Pipe readInOrder();

    template<typename TSource>
    ProcessorPtr createSource(const RangesInDataPart & part);
};

}
