#include <Core/TypedQueryParameters.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_QUERY_PARAMETER;
    extern const int TOO_MANY_COLUMNS;
}

namespace
{

void validateType(const IDataType & type, size_t depth)
{
    if (depth > MAX_TYPED_QUERY_PARAMETER_NESTING_DEPTH)
        throw Exception(
            ErrorCodes::BAD_QUERY_PARAMETER,
            "Binary query parameter type {} exceeds the maximum nesting depth of {}",
            type.getName(),
            MAX_TYPED_QUERY_PARAMETER_NESTING_DEPTH);

    const WhichDataType which(type);
    if (which.isAggregateFunction() || which.isFunction() || which.isDynamic() || which.isObject() || which.isVariant() || which.isSet()
        || which.isQBit() || type.getCustomSerialization())
        throw Exception(
            ErrorCodes::BAD_QUERY_PARAMETER,
            "Type {} cannot be used as a binary query parameter",
            type.getName());

    type.forEachChild([&](const IDataType & child) { validateType(child, depth + 1); });
}

}

void validateTypedQueryParameterType(const IDataType & type)
{
    validateType(type, 0);
}

void validateTypedQueryParameters(const TypedQueryParameters & parameters, const NameToNameMap * text_parameters)
{
    if (parameters.size() > MAX_TYPED_QUERY_PARAMETERS)
        throw Exception(
            ErrorCodes::TOO_MANY_COLUMNS,
            "The number of binary query parameters ({}) exceeds the maximum of {}",
            parameters.size(),
            MAX_TYPED_QUERY_PARAMETERS);

    size_t total_bytes = 0;
    for (const auto & [name, parameter] : parameters)
    {
        if (name.empty())
            throw Exception(ErrorCodes::BAD_QUERY_PARAMETER, "Binary query parameter name cannot be empty");
        if (!parameter.type || !parameter.column)
            throw Exception(ErrorCodes::BAD_QUERY_PARAMETER, "Binary query parameter {} has no type or column", backQuote(name));
        if (parameter.column->size() != 1)
            throw Exception(
                ErrorCodes::BAD_QUERY_PARAMETER,
                "Binary query parameter {} must contain exactly one row, got {}",
                backQuote(name),
                parameter.column->size());
        if (text_parameters && text_parameters->contains(name))
            throw Exception(
                ErrorCodes::BAD_QUERY_PARAMETER,
                "Query parameter {} is specified in both text and binary form",
                backQuote(name));

        validateTypedQueryParameterType(*parameter.type);
        total_bytes += parameter.column->byteSize();
        if (total_bytes > MAX_TYPED_QUERY_PARAMETER_UNCOMPRESSED_BYTES)
            throw Exception(
                ErrorCodes::BAD_QUERY_PARAMETER,
                "Binary query parameters use {} uncompressed bytes, exceeding the maximum of {}",
                total_bytes,
                MAX_TYPED_QUERY_PARAMETER_UNCOMPRESSED_BYTES);
    }
}

String calculateTypedQueryParameterHash(const IDataType & type, const IColumn & column)
{
    SipHash hash;
    const String type_name = type.getName();
    hash.update(type_name.size());
    hash.update(type_name.data(), type_name.size());
    column.updateHashWithValue(0, hash);
    return getSipHash128AsHexString(hash);
}

String makeTypedQueryParameterScalarName(const String & name, const String & value_hash)
{
    String result(1, '\0');
    result += "query_parameter:";
    result += name;
    result += ':';
    result += value_hash;
    return result;
}

}
