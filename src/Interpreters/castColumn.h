#pragma once

#include <Core/ColumnWithTypeAndName.h>

#include <Functions/FunctionsConversion.h>

namespace DB
{

template <CastType cast_type = CastType::nonAccurate>
ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type)
{

    if (arg.type->equals(*type))
        return arg.column;

    ColumnsWithTypeAndName arguments
    {
        arg,
        {
            DataTypeString().createColumnConst(arg.column->size(), type->getName()),
            std::make_shared<DataTypeString>(),
            ""
        }
    };

    FunctionOverloadResolverPtr func_builder_cast =
        std::make_shared<FunctionOverloadResolverAdaptor>(CastOverloadResolver<cast_type>::createImpl(false));

    auto func_cast = func_builder_cast->build(arguments);

    if constexpr (cast_type == CastType::accurateOrNull)
    {
        return func_cast->execute(arguments, makeNullable(type), arg.column->size());
    }
    else
    {
        return func_cast->execute(arguments, type, arg.column->size());
    }
}

}
