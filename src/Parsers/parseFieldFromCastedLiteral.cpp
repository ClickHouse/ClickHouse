#include <Parsers/parseFieldFromCastedLiteral.h>

#include <Common/FieldVisitorToString.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/Serializations/SerializationDecimal.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTHelpers.h>
#include <Parsers/ASTLiteral.h>

#include <base/Decimal_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


namespace
{

/// Recognise `<prefix>(<integer>)` and extract the integer in parentheses as `scale`.
bool tryParseDecimalTypeName(std::string_view type_name, std::string_view prefix, UInt32 & scale)
{
    return type_name.size() >= prefix.size() + 3
        && type_name.starts_with(prefix)
        && type_name[prefix.size()] == '('
        && type_name.back() == ')'
        && tryParse(scale, type_name.substr(prefix.size() + 1, type_name.size() - prefix.size() - 2));
}

/// Identity-return if `src` is already a `T`; otherwise parse its textual form as `T`.
template <typename T>
Field convertFieldTo(const Field & src)
{
    if (src.getType() == Field::TypeToEnum<T>::value)
        return src;
    if (src.getType() == Field::Types::String)
        return parseFromString<T>(src.safeGet<String>());
    return parseFromString<T>(applyVisitor(FieldVisitorToString{}, src));
}

template <is_decimal DecimalT>
Field convertFieldTo(const Field & src, UInt32 scale)
{
    if (src.getType() == Field::TypeToEnum<DecimalField<DecimalT>>::value
        && src.safeGet<DecimalField<DecimalT>>().getScale() == scale)
        return src;

    DecimalT decimal{};
    if (src.getType() == Field::Types::String)
    {
        ReadBufferFromString buf(src.safeGet<String>());
        SerializationDecimal<DecimalT>::readText(decimal, buf, DecimalUtils::max_precision<DecimalT>, scale);
    }
    else
    {
        String str = applyVisitor(FieldVisitorToString(), src);
        ReadBufferFromString buf(str);
        SerializationDecimal<DecimalT>::readText(decimal, buf, DecimalUtils::max_precision<DecimalT>, scale);
    }
    return DecimalField<DecimalT>(decimal, scale);
}

/// Parses `literal_value::type_name` into a Field. See header doc for accepted type names.
Field parseLiteralCastedTo(const Field & literal_value, std::string_view type_name)
{
    UInt32 scale = 0;
    if (tryParseDecimalTypeName(type_name, "Decimal32",  scale))
        return convertFieldTo<Decimal32>(literal_value,  scale);
    if (tryParseDecimalTypeName(type_name, "Decimal64",  scale))
        return convertFieldTo<Decimal64>(literal_value,  scale);
    if (tryParseDecimalTypeName(type_name, "Decimal128", scale))
        return convertFieldTo<Decimal128>(literal_value, scale);
    if (tryParseDecimalTypeName(type_name, "Decimal256", scale))
        return convertFieldTo<Decimal256>(literal_value, scale);

    if (type_name == "Bool")
        return convertFieldTo<bool>(literal_value);
    if (type_name == "Float64")
        return convertFieldTo<Float64>(literal_value);
    if (type_name == "String")
        return convertFieldToString(literal_value);

    if (type_name == "Int64")
        return convertFieldTo<Int64>(literal_value);
    if (type_name == "UInt64")
        return convertFieldTo<UInt64>(literal_value);
    if (type_name == "Int128")
        return convertFieldTo<Int128>(literal_value);
    if (type_name == "UInt128")
        return convertFieldTo<UInt128>(literal_value);
    if (type_name == "Int256")
        return convertFieldTo<Int256>(literal_value);
    if (type_name == "UInt256")
        return convertFieldTo<UInt256>(literal_value);

    if (type_name == "UUID")
        return convertFieldTo<UUID>(literal_value);
    if (type_name == "IPv4")
        return convertFieldTo<IPv4>(literal_value);
    if (type_name == "IPv6")
        return convertFieldTo<IPv6>(literal_value);

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type name '{}' in CAST", type_name);
}

}


Field parseFieldFromCastedLiteral(const ASTPtr & ast)
{
    if (const auto * literal = ast->as<ASTLiteral>())
        return literal->value;

    if (const auto * cast_func = ast->as<ASTFunction>();
        isFunctionCast(cast_func) && cast_func->arguments && cast_func->arguments->children.size() == 2)
    {
        const auto * value_lit = cast_func->arguments->children[0]->as<ASTLiteral>();
        const auto * type_lit  = cast_func->arguments->children[1]->as<ASTLiteral>();
        if (value_lit && type_lit && type_lit->value.getType() == Field::Types::String)
            return parseLiteralCastedTo(value_lit->value, type_lit->value.safeGet<String>());
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Expected a literal or a CAST of a literal, got '{}'", ast->formatForErrorMessage());
}

}
