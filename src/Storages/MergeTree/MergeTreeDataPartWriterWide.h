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

    /// Write single granule of one column (rows between 2 marks)
    void writeSingleGranule(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        IDataType::SerializeBinaryBulkSettings & serialize_settings,
        size_t from_row,
        size_t number_of_rows,
        bool write_marks);


    void flushMarkToFile(
        const StreamNameAndMark & stream_with_mark,
        size_t rows_in_mark);

    StreamsWithMarks getCurrentMarksForColumn(
        const String & name,
        const IDataType & type,
        WrittenOffsetColumns & offset_columns,
        DB::IDataType::SubstreamPath & path);

    /// Write mark for column
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

    void validateColumnOfFixedSize(const String & name, const IDataType & type);

    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;

    IDataType::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns) const;

    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::unordered_map<String, SerializationState>;

    SerializationStates serialization_states;

    using ColumnStreams = std::map<String, StreamPtr>;
    ColumnStreams column_streams;

    size_t rows_written_in_last_mark = 0;
};

}
