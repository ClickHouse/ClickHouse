#pragma once

#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Formats/MarkInCompressedFile.h>
#include <DataTypes/Serializations/ISerialization.h>


namespace DB
{

struct StreamNameAndMark
{
    String stream_name;
    MarkInCompressedFile mark{};
};

using StreamsWithMarks = std::vector<StreamNameAndMark>;
using ColumnNameToMark = std::unordered_map<String, StreamsWithMarks>;

/// Writes data part in wide format.
class MergeTreeDataPartWriterWide : public MergeTreeDataPartWriterOnDisk
{
    using Base = MergeTreeDataPartWriterOnDisk;

public:
    MergeTreeDataPartWriterWide(
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        MergeTreeIndexGranularityPtr index_granularity_,
        WrittenOffsetSubstreams * written_offset_substreams_);

    void write(const Block & block, const IColumnPermutation * permutation, Block * permuted_columns_cache) override;

    void finalizeIndexGranularity() final;
    void fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove) final;

    void finish(bool sync) final;
    void cancel() noexcept override;

    size_t getNumberOfOpenStreams() const override { return column_streams.size(); }

    static ISerialization::EnumerateStreamsSettings getEnumerateSettings(const MergeTreeWriterSettings & settings);

private:
    /// Finish serialization of data: write final mark if required and compute checksums
    /// Also validate written data in debug mode
    void fillDataChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove);
    void finishDataSerialization(bool sync);

    /// Write data of one column.
    /// Return how many marks were written and
    /// how many rows were written for last mark
    void writeColumn(
        const NameAndTypePair & name_and_type,
        const IColumn & column,
        WrittenOffsetSubstreams & offset_substreams,
        const Granules & granules);

    /// Write single granule of one column.
    void writeSingleGranule(
        const NameAndTypePair & name_and_type,
        const IColumn & column,
        const WrittenOffsetSubstreams & offset_substreams,
        ISerialization::SerializeBinaryBulkStatePtr & serialization_state,
        ISerialization::SerializeBinaryBulkSettings & serialize_settings,
        const Granule & granule);

    /// Take offsets from column and return as MarkInCompressed file with stream name.
    /// `offset_substreams` are the offsets streams already written in this block, skipped here.
    StreamsWithMarks getCurrentMarksForColumn(
        const NameAndTypePair & name_and_type,
        const WrittenOffsetSubstreams & offset_substreams);

    /// Write mark to disk using stream and rows count
    void flushMarkToFile(const StreamNameAndMark & stream_with_mark, size_t rows_in_mark);

    /// Write mark for column taking offsets from column stream
    void writeSingleMark(
        const NameAndTypePair & name_and_type,
        const WrittenOffsetSubstreams & offset_substreams,
        size_t number_of_rows);

    void writeFinalMark(
        const NameAndTypePair & name_and_type,
        WrittenOffsetSubstreams & offset_substreams);

    void addStreams(const NameAndTypePair & name_and_type) override;

    /// The stream count is derived from the substreams inventory and read by addStreams(), so the
    /// three initializations must happen in this order.
    void initStreamsAndSubstreamsIfNeeded();

    void initStreamsToOpenCount();

    /// Method for self check (used in debug-build only). Checks that written
    /// data and corresponding marks are consistent. Otherwise throws logical
    /// errors.
    void validateColumnOfFixedSize(const NameAndTypePair & name_type);

    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;

    /// Use information from just written granules to shift current mark
    /// in our index_granularity array.
    void shiftCurrentMark(const Granules & granules_written);

    /// Change rows in the last mark in index_granularity to new_rows_in_last_mark.
    /// Flush all marks from last_non_written_marks to disk and increment current mark if already written rows
    /// (rows_written_in_last_granule) equal to new_rows_in_last_mark.
    ///
    /// This function used when blocks change granularity drastically and we have unfinished mark.
    /// Also useful to have exact amount of rows in last (non-final) mark.
    void adjustLastMarkIfNeedAndFlushToDisk(size_t new_rows_in_last_mark);

    /// Gives every column an empty serialize call with granule_is_complete set, for a granule
    /// that adjustLastMarkIfNeedAndFlushToDisk completed without a call carrying its rows.
    void completeGranuleWithoutRows();

    ISerialization::SerializeBinaryBulkSettings getSerializationSettings() const override;
    /// Settings with the getters and callbacks bound to the streams of the column.
    ISerialization::SerializeBinaryBulkSettings getSerializationSettingsForColumn(
        const NameAndTypePair & name_and_type, const WrittenOffsetSubstreams & offset_substreams) const;

    ISerialization::OutputStreamGetter createStreamGetter(const NameAndTypePair & column,
        const WrittenOffsetSubstreams & offset_substreams) const;

    /// A stream of a column resolved once in `addStreams`; its name is `stream->escaped_column_name`. The list of
    /// a column is in the order of `enumerateStreams`, without the ephemeral substreams and without the offsets
    /// streams written before this writer (`written_offset_substreams`).
    struct ResolvedStream
    {
        MergeTreeWriterStream * stream = nullptr;
        /// ArraySizes substream; the columns of a Nested type share it and write it once per block.
        bool is_offsets = false;
    };
    using ResolvedStreams = std::vector<ResolvedStream>;

    const ResolvedStreams & getResolvedStreams(const String & column_name) const;

    /// The stream of `substream_path` of `column`, or null for an offsets stream written before this writer
    /// (`written_offset_substreams`). Throws for a stream `addStreams` did not register.
    const ResolvedStream * resolveStream(const NameAndTypePair & column, const ISerialization::SubstreamPath & substream_path) const;

    using SerializationState = ISerialization::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::unordered_map<String, SerializationState>;

    SerializationStates serialization_states;

    using ColumnStreams = std::map<String, StreamPtr>;
    ColumnStreams column_streams;

    /// The streams of every column, filled in addStreams. The set of streams of a column and their names do not
    /// change while the part is written (they are enumerated from the fixed `block_sample`), so the per-granule
    /// functions iterate this list instead of enumerating the substreams and building the stream names again.
    std::unordered_map<String, ResolvedStreams> resolved_streams_by_column;

    /// The same streams by full stream name, for the getters the serialization calls with a substream path.
    std::unordered_map<String, ResolvedStream> resolved_streams_by_full_name;

    /// Full names of the offsets streams written before this writer (`written_offset_substreams`). They have no
    /// stream in this writer, and `resolveStream` returns null for them instead of failing.
    NameSet written_before_full_names;

    /// Some long stream names are replaced with hashes. Maps the actual stream name, possibly a hash, to the full one.
    std::unordered_map<String, String> stream_name_to_full_name;

    /// Non written marks to disk (for each column). Waiting until all rows for
    /// this marks will be written to disk.
    using MarksForColumns = std::unordered_map<String, StreamsWithMarks>;
    MarksForColumns last_non_written_marks;

    /// Set of columns to put marks in cache during write.
    NameSet columns_to_load_marks;

    /// How many rows we have already written in the current mark.
    /// More than zero when incoming blocks are smaller then their granularity.
    size_t rows_written_in_last_mark = 0;

    /// Number of streams opened for the whole part. Peak write memory scales with this rather than
    /// with the number of columns, since one column can own thousands of substreams.
    std::optional<size_t> streams_to_open_in_part;
};

}
