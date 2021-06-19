#include <DataStreams/ConvertUserDefinedTypeOutputStream.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

void ConvertUserDefinedTypeOutputStream::write(const Block & block)
{
    Block header_with_user_defined_types = stream->getHeader();
    Block block_to_write;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto& column = block.getByPosition(i);
        ColumnWithTypeAndName column_to_write(column.column, header_with_user_defined_types.getByPosition(i).type, column.name);
        block_to_write.insert(column_to_write);
    }
    stream->write(block_to_write);
}

Block ConvertUserDefinedTypeOutputStream::getHeader() const
{
    return convertUserDefinedTypeToNested(stream->getHeader());
}

Block ConvertUserDefinedTypeOutputStream::convertUserDefinedTypeToNested(const Block & block)
{
    Block new_block;
    for (const auto& column : block)
    {
        if (DataTypeFactory::instance().isUserDefinedDataType(column.type->getFamilyName()))
        {
            DataTypePtr nested = static_cast<const UserDefinedDataType *>(column.type.get())->getNested();
            new_block.insert(ColumnWithTypeAndName(column.column, nested, column.name));
        }
        else
            new_block.insert(column);
    }
    return new_block;
}

}
