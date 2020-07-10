#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

/// Writes data part in wide format.
class MergeTreeDataPartWriterWide : public IMergeTreeDataPartWriter
{
public:

    using ColumnToSize = std::map<std::string, UInt64>;

    MergeTreeDataPartWriterWide(
        const MergeTreeData::DataPartPtr & data_part,
        const NamesAndTypesList & columns_list,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    void write(const Block & block, const IColumn::Permutation * permutation,
        const Block & primary_key_block, const Block & skip_indexes_block) override;

    void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums) override;

    IDataType::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns);

private:
    /// Write data of one column.
    /// Return how many marks were written and
    /// how many rows were written for last mark
    void writeColumn(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns);

    /// Write single granule of one column (rows between 2 marks)
    size_t writeSingleGranule(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        IDataType::SerializeBinaryBulkSettings & serialize_settings,
        size_t from_row,
        size_t number_of_rows,
        bool write_marks);

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
        const CompressionCodecPtr & effective_codec,
        size_t estimated_size);

    SerializationStates serialization_states;

    using ColumnStreams = std::map<String, StreamPtr>;
    ColumnStreams column_streams;
};

}
