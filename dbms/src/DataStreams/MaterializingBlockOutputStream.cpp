#include <Columns/ColumnConst.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <ext/range.h>


namespace DB
{

Block MaterializingBlockOutputStream::materialize(const Block & original_block)
{
    /// copy block to get rid of const
    auto block = original_block;

    for (const auto i : ext::range(0, block.columns()))
    {
        auto & element = block.safeGetByPosition(i);
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

    return block;
}

}
