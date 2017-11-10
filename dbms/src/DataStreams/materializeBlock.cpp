#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

Block materializeBlock(const Block & block)
{
    if (!block)
        return block;

    Block res = block;
    size_t columns = res.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & element = res.getByPosition(i);
        auto & src = element.column;
        ColumnPtr converted = src->convertToFullColumnIfConst();
        if (converted)
        {
            src = converted;

            auto & type = element.type;
            if (type->isNull())
            {
                /// A ColumnNull that is converted to a full column
                /// has the type DataTypeNullable(DataTypeUInt8).
                type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
            }
        }
    }

    return res;
}

}
