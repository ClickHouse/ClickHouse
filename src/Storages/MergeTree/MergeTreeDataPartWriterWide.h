#pragma once
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>

namespace DB
{

struct StreamNameAndMark
{
    String stream_name;
    MarkInCompressedFile mark;
};

using StreamsWithMarks = std::vector<StreamNameAndMark>;
using ColumnNameToMark = std::unordered_map<String, StreamsWithMarks>;

/// Writes data part in wide format.
class MergeTreeDataPartWriterWide : public MergeTreeDataPartWriterOnDisk
{
public:
    MergeTreeDataPartWriterWide(
        const MergeTreeData::DataPartPtr & data_part,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    void write(const Block & block, const IColumn::Permutation * permutation) override;

    void finish(IMergeTreeDataPart::Checksums & checksums, bool sync) final;

private:
    /// Finish serialization of data: write final mark if required and compute checksums
    /// Also validate written data in debug mode
    void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync);

    /// Write data of one column.
    /// Return how many marks were written and
    /// how many rows were written for last mark
    void writeColumn(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        const Granules & granules);

    /// Write single granule of one column.
    void writeSingleGranule(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        IDataType::SerializeBinaryBulkSettings & serialize_settings,
        const Granule & granule);

    /// Take offsets from column and return as MarkInCompressed file with stream name
    StreamsWithMarks getCurrentMarksForColumn(
        const String & name,
        const IDataType & type,
        WrittenOffsetColumns & offset_columns,
        DB::IDataType::SubstreamPath & path);

    /// Write mark to disk using stream and rows count
    void flushMarkToFile(
        const StreamNameAndMark & stream_with_mark,
        size_t rows_in_mark);

    /// Write mark for column taking offsets from column stream
    void writeSingleMark(
        const String & name,
        const IDataType & type,
        WrittenOffsetColumns & offset_columns,
        size_t number_of_rows,
        DB::IDataType::SubstreamPath & path);

    void writeFinalMark(
        const std::string & column_name,
        const DataTypePtr column_type,
        WrittenOffsetColumns & offset_columns,
        DB::IDataType::SubstreamPath & path);

    void addStreams(
        const String & name,
        const IDataType & type,
        const ASTPtr & effective_codec_desc,
        size_t estimated_size);

    /// Method for self check (used in debug-build only). Checks that written
    /// data and corresponding marks are consistent. Otherwise throws logical
    /// errors.
    void validateColumnOfFixedSize(const String & name, const IDataType & type);

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

    IDataType::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns) const;

    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::unordered_map<String, SerializationState>;

    SerializationStates serialization_states;

    using ColumnStreams = std::map<String, StreamPtr>;
    ColumnStreams column_streams;
    /// Non written marks to disk (for each column). Waiting until all rows for
    /// this marks will be written to disk.
    using MarksForColumns = std::unordered_map<String, StreamsWithMarks>;
    MarksForColumns last_non_written_marks;

    /// How many rows we have already written in the current mark.
    /// More than zero when incoming blocks are smaller then their granularity.
    size_t rows_written_in_last_mark = 0;
};

}
