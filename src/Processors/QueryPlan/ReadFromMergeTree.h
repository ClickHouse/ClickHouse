#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Pipe.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>

namespace DB
{

/// Create source from prepared pipe.
class ReadFromMergeTree : public ISourceStep
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

    struct IndexStat
    {
        IndexType type;
        std::string name;
        std::string description;
        size_t num_parts_after;
        size_t num_granules_after;
    };

    using IndexStats = std::vector<IndexStat>;
    using IndexStatPtr = std::unique_ptr<IndexStats>;

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
        Default,
        InOrder,
        InReverseOrder,
    };

    explicit ReadFromMergeTree(
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
