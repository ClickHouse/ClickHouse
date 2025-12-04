#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeProjectionsIndexesTask.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Core/SortCursor.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

struct InvertedIndexSegment
{
    InvertedIndexSegment(DataPartStoragePtr part_storage_, String index_file_name_, size_t part_index_)
        : part_storage(std::move(part_storage_))
        , index_file_name(std::move(index_file_name_))
        , part_index(part_index_)
    {
    }

    DataPartStoragePtr part_storage;
    String index_file_name;
    size_t part_index;
};

class BuildInvertedIndexTransform : public ISimpleTransform
{
public:
    BuildInvertedIndexTransform(
        SharedHeader header,
        String index_file_prefix_,
        std::vector<MergeTreeIndexPtr> indexes_,
        MutableDataPartStoragePtr temporary_storage_,
        MergeTreeWriterSettings writer_settings_,
        CompressionCodecPtr default_codec_,
        String marks_file_extension_);

    String getName() const override { return "BuildInvertedIndexTransform"; }

    IProcessor::Status prepare() override;
    void transform(Chunk & chunk) override;

    void aggregate(const Block & block);
    void finalize();

    std::vector<InvertedIndexSegment> getSegments(size_t index_idx, size_t part_idx) const;
    const std::vector<MergeTreeIndexPtr> & getIndexes() const { return indexes; }

private:
    void writeTemporarySegment(size_t i);

    String index_file_prefix;
    std::vector<MergeTreeIndexPtr> indexes;
    MergeTreeIndexAggregators aggregators;
    MutableDataPartStoragePtr temporary_storage;
    MergeTreeWriterSettings writer_settings;
    CompressionCodecPtr default_codec;
    String marks_file_extension;

    size_t num_processed_rows = 0;
    std::vector<size_t> segment_numbers;
};

class MergeInvertedIndexesTask : public MergeProjectionsIndexesTask
{
public:
    MergeInvertedIndexesTask(
        std::vector<InvertedIndexSegment> segments,
        MergeTreeMutableDataPartPtr new_data_part_,
        MergeTreeIndexPtr index_ptr_,
        std::shared_ptr<MergedPartOffsets> merged_part_offsets_,
        const MergeTreeReaderSettings & reader_settings_,
        const MergeTreeWriterSettings & writer_settings_);

    ~MergeInvertedIndexesTask() noexcept override;

    bool executeStep() override;
    void cancel() noexcept override;

    void addTemporaryFilesCleanupOps(DiskTransactionPtr) noexcept override {}
    void addToChecksums(MergeTreeDataPartChecksums & checksums) override;

private:
    void init();
    void finalize();
    void cancelImpl() noexcept;
    Block getHeader() const;
    void initializeQueue();
    bool isNewToken(const SortCursor & cursor) const;

    void readDictionaryBlock(size_t source_num);
    std::vector<PostingListPtr> readPostingLists(size_t source_num);
    PostingListPtr adjustPartOffsets(size_t source_num, PostingListPtr posting_list);

    void flushPostingList();
    void flushDictionaryBlock();

    std::vector<InvertedIndexSegment> segments;
    MergeTreeMutableDataPartPtr new_data_part;
    MergeTreeIndexPtr index_ptr;
    MergeTreeIndexTextParams params;
    std::shared_ptr<MergedPartOffsets> merged_part_offsets;
    MergeTreeWriterSettings writer_settings;
    size_t step_time_ms;

    std::vector<MergeTreeIndexInputStreams> input_streams;
    std::vector<std::unique_ptr<MergeTreeIndexReaderStream>> input_streams_holders;

    MergeTreeIndexOutputStreams output_streams;
    std::vector<std::unique_ptr<MergeTreeIndexWriterStream>> output_streams_holders;

    SortCursorImpls cursors;
    std::vector<DictionaryBlock> inputs;
    SortingQueue<SortCursor> queue;

    MutableColumnPtr output_tokens;
    PostingList output_postings;
    std::vector<TokenPostingsInfo> output_infos;
    MutableColumnPtr sparse_index_tokens;
    MutableColumnPtr sparse_index_offsets;

    bool is_initialized = false;
};

using MergeInvertedIndexesTaskPtr = std::unique_ptr<MergeInvertedIndexesTask>;

MutableDataPartStoragePtr createTemporaryInvertedIndexStorage(const DiskPtr & disk, const String & part_relative_path);

std::vector<MergeTreeIndexPtr> getInvertedIndexesToBuild(
    const IndicesDescription & indices_description,
    const NameSet & read_column_names,
    const IMergeTreeDataPart & data_part,
    bool merge_may_reduce_rows);

std::unique_ptr<MergeTreeReaderStream> makeInvertedIndexInputStream(
    DataPartStoragePtr data_part_storage,
    const String & stream_name,
    const String & extension,
    const MergeTreeReaderSettings & reader_settings);

}
