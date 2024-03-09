#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/CastOverloadResolverImpl.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

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
    return CastOverloadResolverImpl::create(ContextPtr{}, type, true, diagnostic);
}

REGISTER_FUNCTION(CastOverloadResolvers)
{
    factory.registerFunction("_CAST", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::nonAccurate, true, {}); }, {}, FunctionFactory::CaseInsensitive);
    /// Note: "internal" (not affected by null preserving setting) versions of accurate cast functions are unneeded.

    factory.registerFunction("CAST", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::nonAccurate, false, {}); }, {}, FunctionFactory::CaseInsensitive);
    factory.registerFunction("accurateCast", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::accurate, false, {}); }, {});
    factory.registerFunction("accurateCastOrNull", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::accurateOrNull, false, {}); }, {});
}

}
