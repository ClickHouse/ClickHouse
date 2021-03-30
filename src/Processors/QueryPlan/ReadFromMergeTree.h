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

    explicit ReadFromMergeTree(
        const MergeTreeData & storage_,
        StorageMetadataPtr metadata_snapshot_,
        String query_id_,
        Names required_columns_,
        RangesInDataParts parts_,
        PrewhereInfoPtr prewhere_info_,
        Names virt_column_names_,
        Settings settings_,
        size_t num_streams_,
        bool allow_mix_streams_,
        bool read_reverse_
    );

    String getName() const override { return "ReadFromMergeTree"; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

private:
    const MergeTreeData & storage;
    StorageMetadataPtr metadata_snapshot;
    String query_id;

    Names required_columns;
    RangesInDataParts parts;
    PrewhereInfoPtr prewhere_info;
    Names virt_column_names;
    Settings settings;

    size_t num_streams;
    bool allow_mix_streams;
    bool read_reverse;

    Pipe read();
    Pipe readFromPool();
    Pipe readFromSeparateParts();

    template<typename TSource>
    ProcessorPtr createSource(const RangesInDataPart & part);
};

}
