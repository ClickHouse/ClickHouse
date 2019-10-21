#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

class MergeTreeDataPartWriterCompact : IMergeTreeDataPartWriter
{
public:
    size_t write(const Block & block, size_t from_mark, size_t index_offset, const MergeTreeIndexGranularity & index_granularity,
        const Block & primary_key_block, const Block & skip_indexes_block) override;

    /// Write single granule of one column (rows between 2 marks)
    size_t writeColumnSingleGranule(
        const ColumnWithTypeAndName & column,
        size_t from_row,
        size_t number_of_rows);
    
private:
    ColumnStreamPtr stream;
};

}