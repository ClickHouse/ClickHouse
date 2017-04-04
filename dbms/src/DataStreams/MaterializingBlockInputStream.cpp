#include <DataStreams/MaterializingBlockInputStream.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

MaterializingBlockInputStream::MaterializingBlockInputStream(BlockInputStreamPtr input_)
{
    children.push_back(input_);
}

String MaterializingBlockInputStream::getName() const
{
    return "Materializing";
}

String MaterializingBlockInputStream::getID() const
{
    std::stringstream res;
    res << "Materializing(" << children.back()->getID() << ")";
    return res.str();
}

Block MaterializingBlockInputStream::readImpl()
{
    Block res = children.back()->read();

    if (!res)
        return res;

    size_t columns = res.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & element = res.safeGetByPosition(i);
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
