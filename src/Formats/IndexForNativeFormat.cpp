#include <Formats/IndexForNativeFormat.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_INDEX;
}

void IndexOfBlockForNativeFormat::read(ReadBuffer & istr)
{
    readVarUInt(num_columns, istr);
    readVarUInt(num_rows, istr);
    columns.clear();
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto & column = columns.emplace_back();
        readBinary(column.name, istr);
        readBinary(column.type, istr);
        readBinary(column.location.offset_in_compressed_file, istr);
        readBinary(column.location.offset_in_decompressed_block, istr);
    }
}

void IndexOfBlockForNativeFormat::write(WriteBuffer & ostr) const
{
    writeVarUInt(num_columns, ostr);
    writeVarUInt(num_rows, ostr);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column = columns[i];
        writeBinary(column.name, ostr);
        writeBinary(column.type, ostr);
        writeBinary(column.location.offset_in_compressed_file, ostr);
        writeBinary(column.location.offset_in_decompressed_block, ostr);
    }
}

IndexOfBlockForNativeFormat IndexOfBlockForNativeFormat::extractIndexForColumns(const NameSet & required_columns) const
{
    if (num_columns < required_columns.size())
        throw Exception("Index contain less than required columns", ErrorCodes::INCORRECT_INDEX);

    IndexOfBlockForNativeFormat res;
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column = columns[i];
        if (required_columns.contains(column.name))
            res.columns.push_back(column);
    }

    if (res.columns.size() < required_columns.size())
        throw Exception("Index contain less than required columns", ErrorCodes::INCORRECT_INDEX);
    if (res.columns.size() > required_columns.size())
        throw Exception("Index contain duplicate columns", ErrorCodes::INCORRECT_INDEX);

    res.num_columns = res.columns.size();
    res.num_rows = num_rows;
    return res;
}


void IndexForNativeFormat::read(ReadBuffer & istr)
{
    blocks.clear();
    while (!istr.eof())
    {
        auto & block = blocks.emplace_back();
        block.read(istr);
    }
}

void IndexForNativeFormat::write(WriteBuffer & ostr) const
{
    for (const auto & block : blocks)
        block.write(ostr);
}

IndexForNativeFormat IndexForNativeFormat::extractIndexForColumns(const NameSet & required_columns) const
{
    IndexForNativeFormat res;
    res.blocks.reserve(blocks.size());
    for (const auto & block : blocks)
        res.blocks.emplace_back(block.extractIndexForColumns(required_columns));
    return res;
}

}
