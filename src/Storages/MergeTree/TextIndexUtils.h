#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeProjectionsIndexesTask.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Storages/MergeTree/TextIndexSegment.h>
#include <Core/SortCursor.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Transform that builds text indexes and periodically flushes their segments
/// into temporary storage, when amount of accumulated data reaches some threshold.
/// Used for materialization of text indexes.
class BuildTextIndexTransform : public ISimpleTransform
{
public:
    BuildTextIndexTransform(
        SharedHeader header,
        String index_file_prefix_,
        std::vector<MergeTreeIndexPtr> indexes_,
        MutableDataPartStoragePtr temporary_storage_,
        MergeTreeWriterSettings writer_settings_,
        CompressionCodecPtr default_codec_,
        String marks_file_extension_);

    String getName() const override { return "BuildTextIndexTransform"; }

    IProcessor::Status prepare() override;
    void transform(Chunk & chunk) override;

    void aggregate(const Block & block);
    void finalize();

    /// Returns all segments created by this transform for the given index and part.
    std::vector<TextIndexSegment> getSegments(const String & index_name, size_t part_idx) const;
    const std::vector<MergeTreeIndexPtr> & getIndexes() const { return indexes; }
    bool hasIndex(const String & index_name) const { return index_position_by_name.contains(index_name); }

private:
    /// Resets current index granule and flush a segment
    /// of the text index to the temporary storage.
    void writeTemporarySegment(size_t i);

    String index_file_prefix;
    std::vector<MergeTreeIndexPtr> indexes;
    std::unordered_map<String, size_t> index_position_by_name;
    MergeTreeIndexAggregators aggregators;
    MutableDataPartStoragePtr temporary_storage;
    MergeTreeWriterSettings writer_settings;
    CompressionCodecPtr default_codec;
    String marks_file_extension;

    /// Number of rows in blocks processed by the transform.
    size_t num_processed_rows = 0;
    /// Number of flushed segments for each index.
    std::vector<size_t> segment_numbers;
};

/// Task that merges text indexes from data parts,
/// or temporary segments of text indexes.
/// Task can recalcute row numbers in the source
/// posting to row numbers in the resulting part.
/// The mapping from old part offsets to the new part offsets is built
/// during the merge of data parts and can be optionally passed to this task.
/// Currently merges all segments in one stage
/// TODO: Implement multi-stage merge to reduce the memory usage.
class MergeTextIndexesTask : public MergeProjectionsIndexesTask
{
public:
    MergeTextIndexesTask(
        std::vector<TextIndexSegment> segments,
        MergeTreeMutableDataPartPtr new_data_part_,
        MergeTreeIndexPtr index_ptr_,
        std::shared_ptr<MergedPartOffsets> merged_part_offsets_,
        const MergeTreeReaderSettings & reader_settings_,
        const MergeTreeWriterSettings & writer_settings_);

    ~MergeTextIndexesTask() noexcept override;

    bool executeStep() override;
    void cancel() noexcept override;

    MutableDataPartsVector extractTemporaryParts() override { return {}; }
    void addToChecksums(MergeTreeDataPartChecksums & checksums) override;

private:
    void finalize();
    void cancelImpl() noexcept;
    Block getHeader() const;
    void initializeQueue();

    /// Returns true if the given cursor points to a new token.
    bool isNewToken(const SortCursor & cursor) const;
    /// Reads the next dictionary block for the given source index.
    void readDictionaryBlock(size_t source_num);
    /// Reads the next posting lists for the next token in the given source index.
    std::vector<PostingListPtr> readPostingLists(size_t source_num);
    /// Adjusts row numbers in the postings list according to merged part offsets.
    PostingListPtr adjustPartOffsets(size_t source_num, PostingListPtr posting_list);

    void flushPostingList();
    void flushDictionaryBlock();

    std::vector<TextIndexSegment> segments;
    MergeTreeMutableDataPartPtr new_data_part;
    MergeTreeIndexPtr index_ptr;
    MergeTreeIndexTextParams params;

    /// If not null, posting list values must be recalculated using merged offsets.
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

    /// Tokens accumulated for the current dictionary block.
    MutableColumnPtr output_tokens;
    /// Tokens infos accumulated for the current dictionary block.
    std::vector<TokenPostingsInfo> output_infos;
    /// Postings accumulated for the current token.
    PostingList output_postings;
    /// Sparse index accumulated for the task. Flushed only once in the end of the task.
    MutableColumnPtr sparse_index_tokens;
    MutableColumnPtr sparse_index_offsets;

    PostingListCodecPtr posting_list_codec;

    bool is_initialized = false;
};

using MergeTextIndexesTaskPtr = std::unique_ptr<MergeTextIndexesTask>;

MutableDataPartStoragePtr createTemporaryTextIndexStorage(const DiskPtr & disk, const String & part_relative_path);

std::unique_ptr<MergeTreeReaderStream> makeTextIndexInputStream(
    DataPartStoragePtr data_part_storage,
    const String & stream_name,
    const String & extension,
    const MergeTreeReaderSettings & reader_settings);

}
