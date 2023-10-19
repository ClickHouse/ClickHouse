#include <Interpreters/castColumn.h>

#include <Functions/FunctionsConversion.h>
#include <Functions/CastOverloadResolver.h>

namespace DB
{

template <CastType cast_type = CastType::nonAccurate>
static ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr)
{
    if (arg.type->equals(*type) && cast_type != CastType::accurateOrNull)
        return arg.column;

    const auto from_name = arg.type->getName();
    const auto to_name = type->getName();
    ColumnsWithTypeAndName arguments
    {
        arg,
        {
            DataTypeString().createColumnConst(arg.column->size(), to_name),
            std::make_shared<DataTypeString>(),
            ""
        }
    };
    auto get_cast_func = [&arguments]
    {
        FunctionOverloadResolverPtr func_builder_cast = CastInternalOverloadResolver<cast_type>::createImpl();
        return func_builder_cast->build(arguments);
    };

    FunctionBasePtr func_cast = cache ? cache->getOrSet(cast_type, from_name, to_name, std::move(get_cast_func)) : get_cast_func();

    if constexpr (cast_type == CastType::accurateOrNull)
    {
        return func_cast->execute(arguments, makeNullable(type), arg.column->size());
    }
    else
    {
        return func_cast->execute(arguments, type, arg.column->size());
    }
}

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    return castColumn<CastType::nonAccurate>(arg, type, cache);
}

ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    return castColumn<CastType::accurate>(arg, type, cache);
}

ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    return castColumn<CastType::accurateOrNull>(arg, type, cache);
}

}
