#include <DataStreams/TextRowOutputStream.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <DataStreams/materializeBlock.h>


namespace DB
{

// TODO: we can do it better
void TextRowOutputStream::write(const Block & block)
{

    Block b = materializeBlock(block);
    auto & column_string = typeid_cast<const ColumnString &>(*b.safeGetByPosition(0).column);

    for (size_t row_num = 0; row_num < b.rows(); ++row_num)
    {
        writeString(column_string.getDataAt(row_num), wb);
        writeChar('\n', wb);
    }
}

}
