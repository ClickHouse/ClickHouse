#include <Interpreters/castColumn.h>

#include <Functions/FunctionsConversion.h>
#include <Functions/CastOverloadResolver.h>

namespace DB
{

template <CastType cast_type = CastType::nonAccurate>
static ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type)
{
    if (arg.type->equals(*type) && cast_type != CastType::accurateOrNull)
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

    FunctionOverloadResolverPtr func_builder_cast = CastInternalOverloadResolver<cast_type>::createImpl();

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

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type)
{
    return castColumn<CastType::nonAccurate>(arg, type);
}

ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type)
{
    return castColumn<CastType::accurate>(arg, type);
}

ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type)
{
    return castColumn<CastType::accurateOrNull>(arg, type);
}

}
