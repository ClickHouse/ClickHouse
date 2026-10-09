#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeProjectionsIndexesTask.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexPositionData.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Storages/MergeTree/TextIndexSegment.h>
#include <Core/SortCursor.h>
#include <Columns/ColumnString.h>
#include <Processors/ISimpleTransform.h>

#include <span>

namespace DB
{

/// Transform that builds text indexes and periodically flushes their segments
/// into temporary storage, when amount of accumulated data reaches some threshold.
/// Used for materialization of text indexes.
class BuildTextIndexTransform final : public ISimpleTransform
{
public:
    BuildTextIndexTransform(
        SharedHeader header,
        String index_file_prefix_,
        std::vector<MergeTreeIndexPtr> indexes_,
        MutableDataPartStoragePtr temporary_storage_,
        MergeTreeWriterSettings writer_settings_,
        CompressionCodecPtr default_codec_,
        String marks_file_extension_,
        const MergeTreeSettings & storage_settings);

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
    /// Estimated memory retained by each index builder.
    std::vector<size_t> estimated_allocated_bytes;
    size_t max_processed_tokens;
    size_t max_allocated_bytes;
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
        size_t num_rows_,
        MergeTreeIndexPtr index_ptr_,
        std::shared_ptr<MergedPartOffsets> merged_part_offsets_,
        const MergeTreeReaderSettings & reader_settings_,
        const MergeTreeWriterSettings & writer_settings_,
        bool need_fsync_);

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

    /// Cursor over the single String sort column with statically dispatched comparisons.
    using TokenSortCursor = SpecializedSingleColumnSortCursor<ColumnString>;

    /// Returns true if the given cursor points to a new token.
    bool isNewToken(const TokenSortCursor & cursor) const;
    /// Reads the next dictionary block for the given source index.
    void readDictionaryBlock(size_t source_num);
    /// Adjusts the part offset of the given row id according to merged part offsets.
    UInt32 adjustPartOffset(size_t part_index, UInt32 row_id) const;

    /// Unions the given row ids into output_postings_bitmap.
    void appendPostingsToBitmap(std::span<UInt32> row_ids);
    /// Appends the already adjusted row ids of one source to output_postings_array or output_postings_bitmap.
    void appendPostings(size_t source_num, std::span<UInt32> row_ids);
    /// Reads the postings of one source and appends them to output_postings_bitmap or output_postings_array.
    void readAndAppendPostings(size_t source_num, TokenPostingsInfo & token_info);
    /// Reads the positions of one source and appends them to output_positions.
    void readAndAppendPositions(size_t source_num, TokenPostingsInfo & token_info);

    void flushPostingList();
    void flushDictionaryBlock();

    std::vector<TextIndexSegment> segments;
    MergeTreeMutableDataPartPtr new_data_part;
    size_t num_rows;
    MergeTreeIndexPtr index_ptr;
    MergeTreeIndexTextParams params;

    /// If not null, posting list values must be recalculated using merged offsets.
    std::shared_ptr<MergedPartOffsets> merged_part_offsets;
    MergeTreeWriterSettings writer_settings;

    /// Whether to fsync the produced index files in finalize
    bool need_fsync;

    size_t step_time_ms;

    std::vector<MergeTreeIndexInputStreams> input_streams;
    std::vector<std::unique_ptr<MergeTreeIndexReaderStream>> input_streams_holders;

    MergeTreeIndexOutputStreams output_streams;
    std::vector<std::unique_ptr<MergeTreeIndexWriterStream>> output_streams_holders;

    SortCursorImpls cursors;
    std::vector<DictionaryBlock> inputs;
    SortingQueueBatch<TokenSortCursor> queue;

    /// Tokens accumulated for the current dictionary block.
    MutableColumnPtr output_tokens;
    /// Tokens infos accumulated for the current dictionary block.
    std::vector<TokenPostingsInfo> output_infos;
    /// Postings accumulated for the current token when they don't fit into output_postings_array.
    PostingList output_postings_bitmap;
    /// Buffer of at most MAX_CARDINALITY_FOR_RAW_POSTINGS postings of the current token.
    PaddedPODArray<UInt32> output_postings_array;
    /// Reusable buffer for row ids of one posting list block read from a source.
    PaddedPODArray<UInt32> row_ids_buffer;
    /// Reusable buffer for position entries of one token read from a source.
    PODArray<RoaringishEntry> position_entries_buffer;
    /// Positions accumulated for the current token (phrase query support).
    PaddedPODArray<RoaringishEntry> output_positions;
    /// Sparse index accumulated for the task. Flushed only once in the end of the task.
    MutableColumnPtr sparse_index_tokens;
    MutableColumnPtr sparse_index_offsets;

    /// Deserializer for the merged output part, using the destination codec resolved from the index definition.
    PostingsSerialization postings_serialization;
    /// Per-source deserializers, each using the codec read from that source part's own header.
    std::vector<PostingsSerialization> source_postings_serializations;

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
