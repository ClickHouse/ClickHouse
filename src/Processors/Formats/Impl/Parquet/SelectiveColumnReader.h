#pragma once
#include <vector>
#include <Processors/Chunk.h>

namespace DB
{
class RowSet
{};

class SelectiveColumnReader;

using SelectiveColumnReaderPtr = std::shared_ptr<SelectiveColumnReader>;

class SelectiveColumnReader
{
public:
    virtual ~SelectiveColumnReader();
    virtual void computeRowSet(RowSet row_set, int num_rows);
    virtual ColumnPtr read(RowSet row_set, int num_rows) = 0;

    virtual void getValues();
};

class RowGroupChunkReader
{
public:
    Chunk readChunk(int rows);

private:
    std::vector<String> filter_columns;
    std::unordered_map<String, SelectiveColumnReaderPtr> reader_columns_mapping;
    std::vector<SelectiveColumnReaderPtr> column_readers;
};
}




