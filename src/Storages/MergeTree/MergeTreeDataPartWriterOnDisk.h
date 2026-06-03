#pragma once

#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>

namespace DB
{

/// Single unit for writing data to disk. Contains information about
/// amount of rows to write and marks.
struct Granule
{
    /// Start row in block for granule
    size_t start_row;
    /// Amount of rows from block which have to be written to disk from start_row
    size_t rows_to_write;
    /// Global mark number in the list of all marks (index_granularity) for this part
    size_t mark_number;
    /// Should writer write mark for the first of this granule to disk.
    /// NOTE: Sometimes we don't write mark for the start row, because
    /// this granule can be continuation of the previous one.
    bool mark_on_start;
    /// if true: When this granule will be written to disk all rows for corresponding mark will
    /// be wrtten. It doesn't mean that rows_to_write == index_granularity.getMarkRows(mark_number),
    /// We may have a lot of small blocks between two marks and this may be the last one.
    bool is_complete;
};

/// Multiple granules to write for concrete block.
using Granules = std::vector<Granule>;

/// Data-only writer stream for large posting lists (no marks file).
struct LargePostingListWriterStream
{
    LargePostingListWriterStream(
        const String & escaped_column_name_,
        const MutableDataPartStoragePtr & data_part_storage,
        const String & data_path_,
        const std::string & data_file_extension_,
        size_t max_compress_block_size_,
        const WriteSettings & query_write_settings);

    ~LargePostingListWriterStream()
    {
        plain_file.reset();
    }

    void preFinalize();
    void finalize();
    void cancel() noexcept;
    void sync() const;
    void addToChecksums(MergeTreeDataPartChecksums & checksums);

    String escaped_column_name;
    std::string data_file_extension;

    /// compressed_hashing -> compressor -> plain_hashing -> plain_file
    std::unique_ptr<WriteBufferFromFileBase> plain_file;
    HashingWriteBuffer plain_hashing;

    bool is_prefinalized = false;

    alignas(16) UInt32 doc_buffer[TURBOPFOR_BLOCK_SIZE]; // NOLINT(cppcoreguidelines-pro-type-member-init)
    /// Sized for 64-bit TurboPFor worst case (position index uses p4D1Enc256v64).
    uint8_t packed_buffer[TURBOPFOR_MAX_ENCODED_SIZE_64]; // NOLINT(cppcoreguidelines-pro-type-member-init)

    /// Phrase-mode scratch buffer for position delta accumulation across doc blocks.
    /// Not zero-initialized — always overwritten before use.
    /// Non-phrase mode does not use this buffer.
    ///
    /// `packed_buffer` (above) is reused as the TurboPFor encode output buffer for
    /// positions in finish(), and for freq re-encoding in merge() — it is unused by
    /// those paths for its original purpose (tokenize add/finalize already completed).
    alignas(16) UInt32 pos_accum[TURBOPFOR_BLOCK_SIZE]; // NOLINT(cppcoreguidelines-pro-type-member-init)

    /// Phrase-mode scratch buffer for frequency accumulation during merge.
    /// Separate from pos_accum because LargePostingBlockWriter uses pos_accum
    /// for position delta accumulation concurrently with freq buffering.
    alignas(16) UInt32 freq_accum[TURBOPFOR_BLOCK_SIZE]; // NOLINT(cppcoreguidelines-pro-type-member-init)
};

using LargePostingListWriterStreamPtr = std::unique_ptr<LargePostingListWriterStream>;

/// Writes data part to disk in different formats.
/// Calculates and serializes primary and skip indices if needed.
class MergeTreeDataPartWriterOnDisk : public IMergeTreeDataPartWriter
{
public:
    using WrittenOffsetSubstreams = std::set<std::string>;
    using StreamPtr = std::unique_ptr<MergeTreeWriterStream>;

    MergeTreeDataPartWriterOnDisk(
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        MergeTreeIndexGranularityPtr index_granularity_,
        WrittenOffsetSubstreams * written_offset_substreams_);

    void cancel() noexcept override;

    const Block & getColumnsSample() const override { return block_sample; }
    const ColumnsSubstreams & getColumnsSubstreams() const override { return columns_substreams; }

protected:
     /// Count index_granularity for block and store in `index_granularity`
    size_t computeIndexGranularity(const Block & block) const;

    /// Write primary index according to granules_to_write
    void calculateAndSerializePrimaryIndex(const Block & primary_index_block, const Granules & granules_to_write);
    /// Write skip indices according to granules_to_write. Skip indices also have their own marks
    /// and one skip index granule can contain multiple "normal" marks. So skip indices serialization
    /// require additional state: skip_indices_aggregators and skip_index_accumulated_marks
    void calculateAndSerializeSkipIndices(const Block & skip_indexes_block, const Granules & granules_to_write);

    /// Finishes primary index serialization: write final primary index row (if required) and compute checksums
    void fillPrimaryIndexChecksums(MergeTreeDataPartChecksums & checksums);
    void finishPrimaryIndexSerialization(bool sync);
    /// Finishes skip indices serialization: write all accumulated data to disk and compute checksums
    void fillSkipIndicesChecksums(MergeTreeDataPartChecksums & checksums);
    void finishSkipIndicesSerialization(bool sync);

    void fillLargePostingChecksums(MergeTreeDataPartChecksums & checksums);
    void finishLargePostingSerialization(bool sync);

    /// Get global number of the current which we are writing (or going to start to write)
    size_t getCurrentMark() const { return current_mark; }

    void setCurrentMark(size_t mark) { current_mark = mark; }

    /// Get unique non ordered skip indices column.
    Names getSkipIndicesColumns() const;

    virtual void addStreams(const NameAndTypePair & name_and_type, const ASTPtr & effective_codec_desc) = 0;

    /// For some columns the set of streams may depend on the dynamic structure/statistics of the actual column.
    /// Before writing a block we need to prepare its columns, so they will always be serialized in the same
    /// set of streams.
    void prepareBlockForWriting(Block & block);

    /// Initialize all streams for all columns. Should be called after first prepareBlockForWriting when block sample is initialized.
    void initStreamsIfNeeded();

    /// Initialize columns_substreams for all columns. Should be called after first prepareBlockForWriting when block sample is initialized.
    void initColumnsSubstreamsIfNeeded();

    virtual ISerialization::SerializeBinaryBulkSettings getSerializationSettings() const = 0;

    const MergeTreeIndices skip_indices;

    std::unordered_map<String, LargePostingListWriterStreamPtr> large_posting_streams;
    std::unordered_map<String, LargePostingListWriterStreamPtr> position_streams;
    std::unordered_map<String, LargePostingListWriterStreamPtr> lidx_streams;

    const String marks_file_extension;
    const CompressionCodecPtr default_codec;

    const bool compute_granularity;

    std::vector<StreamPtr> skip_indices_streams_holders;
    std::vector<MergeTreeIndexOutputStreams> skip_indices_streams;

    MergeTreeIndexAggregators skip_indices_aggregators;
    std::vector<size_t> skip_index_accumulated_marks;

    std::unique_ptr<WriteBufferFromFileBase> index_file_stream;
    std::unique_ptr<HashingWriteBuffer> index_file_hashing_stream;
    std::unique_ptr<CompressedWriteBuffer> index_compressor_stream;
    std::unique_ptr<HashingWriteBuffer> index_source_hashing_stream;
    bool compress_primary_key;

    /// Last block with index columns.
    /// It's written to index file in the `writeSuffixAndFinalizePart` method.
    Block last_index_block;
    Serializations index_serializations;

    bool data_written = false;

    /// Substreams that should be ignored by this writer, due to they had been written by other writer (as part of vertical merge)
    /// This is to correctly write Nested elements column-by-column.
    WrittenOffsetSubstreams * written_offset_substreams;

    /// Data is already written up to this mark.
    size_t current_mark = 0;

    Block block_sample;

    bool streams_initialized = false;

    /// List of substreams for each column in order of serialization.
    ColumnsSubstreams columns_substreams;

private:
    void initSkipIndices();
    void initPrimaryIndex();

    virtual void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) = 0;
    void calculateAndSerializePrimaryIndexRow(const Block & index_block, size_t row);

    struct ExecutionStatistics
    {
        explicit ExecutionStatistics(size_t skip_indices_cnt) : skip_indices_build_us(skip_indices_cnt, 0)
        {
        }

        std::vector<size_t> skip_indices_build_us; // [i] corresponds to the i-th index
    };

    ExecutionStatistics execution_stats;
    LoggerPtr log;
};

}
