#include "SSDCacheDictionary.h"

#include <Columns/ColumnsNumber.h>

namespace DB
{

BlockFile::BlockFile(size_t file_id, const std::string & file_name, const Block & header, size_t buffer_size)
    : id(file_id), file_name(file_name), buffer_size(buffer_size), out_file(file_name, buffer_size), in_file(file_name), header(header), buffer(header.cloneEmptyColumns())
{
}

void BlockFile::appendBlock(const Block & block)
{
    size_t bytes = 0;
    const auto new_columns = block.getColumns();
    if (new_columns.size() != buffer.size())
    {
        throw Exception("Wrong size of block in BlockFile::appendBlock(). It's a bug.", ErrorCodes::TYPE_MISMATCH);
    }

    const auto id_column = typeid_cast<const ColumnUInt64 *>(new_columns.front().get());
    if (!id_column)
        throw Exception{"id column has type different from UInt64.", ErrorCodes::TYPE_MISMATCH};

    size_t start_size = buffer.front()->size();
    for (size_t i = 0; i < header.columns(); ++i)
    {
        buffer[i]->insertRangeFrom(*new_columns[i], 0, new_columns[i]->size());
        bytes += buffer[i]->byteSize();
    }

    const auto & ids = id_column->getData();
    for (size_t i = 0; i < new_columns.size(); ++i)
    {
        key_to_file_offset[ids[i]] = start_size + i;
    }

    if (bytes >= buffer_size)
    {
        flush();
    }
}

void BlockFile::flush()
{
    const auto id_column = typeid_cast<const ColumnUInt64 *>(buffer.front().get());
    if (!id_column)
        throw Exception{"id column has type different from UInt64.", ErrorCodes::TYPE_MISMATCH};
    const auto & ids = id_column->getData();

    key_to_file_offset[ids[0]] = out_file.getPositionInFile() + (1ULL << FILE_OFFSET_SIZE);
    size_t prev_size = 0;
    for (size_t row = 0; row < buffer.front()->size(); ++row)
    {
        key_to_file_offset[ids[row]] = key_to_file_offset[ids[row ? row - 1 : 0]] + prev_size;
        prev_size = 0;
        for (size_t col = 0; col < header.columns(); ++col)
        {
            const auto & column = buffer[col];
            const auto & type = header.getByPosition(col).type;
            type->serializeBinary(*column, row, out_file);
            if (type->getTypeId() != TypeIndex::String) {
                prev_size += column->sizeOfValueIfFixed();
            } else {
                prev_size += column->getDataAt(row).size + sizeof(UInt64);
            }
        }
    }

    if (out_file.hasPendingData()) {
        out_file.sync();
    }

    buffer = header.cloneEmptyColumns();
}



}
