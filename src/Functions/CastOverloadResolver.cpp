#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/CastOverloadResolverImpl.h>


namespace DB
{

UInt32 extractToDecimalScale(const ColumnWithTypeAndName & named_column)
{
    const auto * arg_type = named_column.type.get();
    bool ok = checkAndGetDataType<DataTypeUInt64>(arg_type)
        || checkAndGetDataType<DataTypeUInt32>(arg_type)
        || checkAndGetDataType<DataTypeUInt16>(arg_type)
        || checkAndGetDataType<DataTypeUInt8>(arg_type);
    if (!ok)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of toDecimal() scale {}", named_column.type->getName());

    Field field;
    named_column.column->get(0, field);
    return static_cast<UInt32>(field.get<UInt32>());
}

FunctionOverloadResolverPtr createInternalCastOverloadResolver(CastType type, std::optional<CastDiagnostic> diagnostic)
{
    switch (type)
    {
        case CastType::nonAccurate:
            return CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(diagnostic);
        case CastType::accurate:
            return CastInternalOverloadResolver<CastType::accurate>::createImpl(diagnostic);
        case CastType::accurateOrNull:
            return CastInternalOverloadResolver<CastType::accurateOrNull>::createImpl(diagnostic);
    }
}

REGISTER_FUNCTION(CastOverloadResolvers)
{
    factory.registerFunction<CastInternalOverloadResolver<CastType::nonAccurate>>({}, FunctionFactory::CaseInsensitive);
    /// Note: "internal" (not affected by null preserving setting) versions of accurate cast functions are unneeded.

    factory.registerFunction<CastOverloadResolver<CastType::nonAccurate>>({}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<CastOverloadResolver<CastType::accurate>>();
    factory.registerFunction<CastOverloadResolver<CastType::accurateOrNull>>();
}

}
