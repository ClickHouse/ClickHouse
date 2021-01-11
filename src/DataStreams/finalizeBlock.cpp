#include <DataStreams/finalizeBlock.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/typeid_cast.h>


namespace DB
{
    void finalizeBlock(Block & block)
    {
        for (size_t i = 0; i < block.columns(); ++i)
        {
            ColumnWithTypeAndName & current = block.getByPosition(i);
            const DataTypeAggregateFunction * unfinalized_type = typeid_cast<const DataTypeAggregateFunction *>(current.type.get());

            if (unfinalized_type)
            {
                current.type = unfinalized_type->getReturnType();
                if (current.column)
                {
                    auto mut_column = IColumn::mutate(std::move(current.column));
                    current.column = ColumnAggregateFunction::convertToValues(std::move(mut_column));
                }
            }
        }
    }
}
