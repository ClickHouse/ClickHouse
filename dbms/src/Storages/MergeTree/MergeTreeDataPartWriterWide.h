#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

class MergeTreeDataPartWriterWide : IMergeTreeDataPartWriter
{
public:
    size_t write(const Block & block, size_t from_mark, size_t index_offset, 
        const MergeTreeIndexGranularity & index_granularity,
        const Block & primary_key_block, const Block & skip_indexes_block) override;

       void addStreams(const String & path, const String & name, const IDataType & type,
                    const CompressionCodecPtr & codec, size_t estimated_size, bool skip_offsets);


    IDataType::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns, bool skip_offsets);

    /// Write data of one column.
    /// Return how many marks were written and
    /// how many rows were written for last mark
    std::pair<size_t, size_t> writeColumn(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        size_t from_mark,
        size_t index_offset);

private:
    /// Write single granule of one column (rows between 2 marks)
    size_t writeSingleGranule(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
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
        bool skip_offsets,
        size_t number_of_rows,
        DB::IDataType::SubstreamPath & path);

    SerializationStates serialization_states;
    bool can_use_adaptive_granularity;
    ColumnStreams column_streams;

};

}