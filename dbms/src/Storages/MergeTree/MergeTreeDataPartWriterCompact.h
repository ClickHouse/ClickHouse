#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

class MergeTreeDataPartWriterCompact : IMergeTreeDataPartWriter
{
public:
    size_t write(const Block & block, size_t from_mark, size_t offset,
        const Block & primary_key_block, const Block & skip_indexes_block) override;

    std::pair<size_t, size_t> writeColumn(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        size_t from_mark) override;

    /// Write single granule of one column (rows between 2 marks)
    size_t writeColumnSingleGranule(
        const ColumnWithTypeAndName & column,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        IDataType::SerializeBinaryBulkSettings & serialize_settings,
        size_t from_row,
        size_t number_of_rows);

    void writeSingleMark()

protected:
    void start() override;

private:
    ColumnStream stream;
    MergeTreeIndexGranularity * index_granularity = nullptr;
    Columns columns_to_write;
};

}