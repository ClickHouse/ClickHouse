#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeProjectionsIndexesTask.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexPositionData.h>
#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Storages/MergeTree/TextIndexSegment.h>
#include <Core/SortCursor.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
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
    /// Builds the merged per-row document lengths and the per-part BM25 collection statistics once,
    /// before token iteration. Only called on the scoring path.
    void buildDocLengthsAndStats();

    /// Adjusts all row ids in place; no-op without merged part offsets.
    void adjustPartOffsets(std::span<UInt32> row_ids, size_t part_index) const;

    /// One source's posting list metadata for the current token; postings are decoded lazily on flush.
    struct TokenSource
    {
        size_t source_num{};
        TokenPostingsInfo info;
    };

    /// Cursor over the single UInt32 row id column with statically dispatched comparisons.
    using PostingsSortCursor = SpecializedSingleColumnSortCursor<ColumnUInt32>;

    /// Streams the sorted (remapped) row ids of one source, one decoded segment at a time.
    struct PostingsMergeCursor
    {
        const TokenSource * source = nullptr;
        /// Next entry of info.offsets to decode.
        size_t next_segment = 0;
        /// Decoded and remapped row ids of the current segment.
        /// The sort cursor points at this column once, in the task constructor.
        ColumnUInt32::MutablePtr column;
        /// Exact per-row term frequencies of the current segment, parallel to the row ids.
        /// Empty when the segment carries none (every `tf == 1`) or the merge is not scoring.
        PaddedPODArray<UInt32> tfs;
        /// Row ids of the source's postings for the current token in pre-remap order, accumulated
        /// across segments. Filled only when the token has positions, which are addressed by posting rank.
        PaddedPODArray<UInt32> token_row_ids;
        SortCursorImpl impl;

        PaddedPODArray<UInt32> & rowIds() { return column->getData(); }
        const PaddedPODArray<UInt32> & rowIds() const { return column->getData(); }

        /// Rewinds the sort cursor to the start of the refilled column.
        void resetToColumnStart()
        {
            impl.rows = column->size();
            impl.getPosRef() = 0;
        }
    };

    /// Points the cursor at a source and decodes its first postings.
    void initCursor(PostingsMergeCursor & cursor, const TokenSource & source);
    /// Decodes the source's next segment; returns false when the source is exhausted.
    bool advanceCursorSegment(PostingsMergeCursor & cursor);

    /// Merges the postings of output_sources and passes sorted non-empty
    /// chunks of row ids to the sink in the globally sorted order.
    /// Once a source is exhausted, its positions (if any) are appended to output_positions.
    template <typename Sink>
    void mergePostings(Sink && sink);

    /// Appends the per-row `(tf - 1)` of one merged chunk to output_tfs_buffer, parallel to the row ids;
    /// an empty `tfs` span means every `tf` of the chunk is 1.
    void appendTermFrequencies(std::span<const UInt32> tfs, size_t num_row_ids);

    /// Serializes a merged posting list of up to MAX_CARDINALITY_FOR_RAW_POSTINGS row ids as raw or embedded postings.
    TokenPostingsInfo flushRawPostings(MergeTreeIndexWriterStream & postings_stream, size_t total_cardinality);
    TokenPostingsInfo flushEncodedPostings(MergeTreeIndexWriterStream & postings_stream, size_t total_cardinality);

    /// Appends the pre-remap row ids of the cursor's current segment to its token_row_ids if the token has positions.
    void captureRowIdsForPositions(PostingsMergeCursor & cursor) const;
    /// Reads the positions of an exhausted source, pairing them with the row ids captured by its
    /// cursor, remaps their row ids and appends them to output_positions.
    void readAndAppendPositions(const PostingsMergeCursor & cursor);
    /// Sorts and merges output_positions and serializes them to the positions stream.
    void flushPositions(TokenPostingsInfo & token_info);

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
    /// Sources of the current token's postings, one per input part or segment.
    std::vector<TokenSource> output_sources;
    /// Reusable buffer for the merged row ids of the current token.
    PaddedPODArray<UInt32> output_postings_buffer;
    /// Merged exact per-row term frequencies of the current token, parallel to
    /// output_postings_buffer. Filled only on the scoring path.
    PaddedPODArray<UInt32> output_tfs_buffer;
    /// Resusable cursors for merging of posting lists.
    std::vector<PostingsMergeCursor> postings_merge_cursors;
    /// Min-queue over the postings cursors of the current token; drained by every mergePostings call.
    SortingQueueBatch<PostingsSortCursor> postings_queue;
    /// Reusable buffer for position entries of one token read from a source.
    PODArray<RoaringishEntry> position_entries_buffer;
    /// Positions accumulated for the current token (phrase query support).
    PaddedPODArray<RoaringishEntry> output_positions;
    /// Reused across tokens to keep position decode allocation-free during merge.
    TextIndexBlockedPositionsCodec::DecodeScratch blocked_decode_scratch;
    /// Sparse index accumulated for the task. Flushed only once in the end of the task.
    MutableColumnPtr sparse_index_tokens;
    MutableColumnPtr sparse_index_offsets;

    /// Deserializer for the merged output part, using the destination codec resolved from the index definition.
    PostingsSerialization postings_serialization;
    /// Per-source deserializers, each using the codec read from that source part's own header.
    std::vector<PostingsSerialization> source_postings_serializations;

    bool is_initialized = false;

    /// BM25 scoring state.
    /// Merged per-row `SmallFloat` document-length bytes, indexed by global merged row id.
    PaddedPODArray<UInt8> merged_doc_lengths;
    /// Total sum of the merged per-row document lengths.
    UInt64 merged_sum_doc_length = 0;
};

using MergeTextIndexesTaskPtr = std::unique_ptr<MergeTextIndexesTask>;

MutableDataPartStoragePtr createTemporaryTextIndexStorage(const DiskPtr & disk, const String & part_relative_path);

std::unique_ptr<MergeTreeReaderStream> makeTextIndexInputStream(
    DataPartStoragePtr data_part_storage,
    const String & stream_name,
    const String & extension,
    const MergeTreeReaderSettings & reader_settings);

}
