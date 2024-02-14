#include <Interpreters/applyFunction.h>

#include <Core/Range.h>
#include <Functions/IFunction.h>

namespace DB
{

static Field applyFunctionForField(const FunctionBasePtr & func, const DataTypePtr & arg_type, const Field & arg_value)
{
    ColumnsWithTypeAndName columns{
        {arg_type->createColumnConst(1, arg_value), arg_type, "x"},
    };

    auto col = func->execute(columns, func->getResultType(), 1);
    return (*col)[0];
}

FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field)
{
    /// Fallback for fields without block reference.
    if (field.isExplicit())
        return applyFunctionForField(func, current_type, field);

    String result_name = "_" + func->getName() + "_" + toString(field.column_idx);
    const auto & columns = field.columns;
    size_t result_idx = columns->size();

    for (size_t i = 0; i < result_idx; ++i)
        if ((*columns)[i].name == result_name)
            result_idx = i;

    if (result_idx == columns->size())
    {
        ColumnsWithTypeAndName args{(*columns)[field.column_idx]};
        field.columns->emplace_back(ColumnWithTypeAndName{nullptr, func->getResultType(), result_name});
        (*columns)[result_idx].column = func->execute(args, (*columns)[result_idx].type, columns->front().column->size());
    }

    return {field.columns, field.row_idx, result_idx};
}

}
