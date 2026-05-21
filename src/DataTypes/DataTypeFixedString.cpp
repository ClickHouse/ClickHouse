#include <Columns/ColumnFixedString.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>

#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationFixedString.h>
#include <DataTypes/Serializations/SerializationFixedStringWithTextRepresentation.h>

#include <IO/WriteHelpers.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

String fixedStringTextRepresentationToString(FixedStringTextRepresentation representation)
{
    switch (representation)
    {
        case FixedStringTextRepresentation::Raw: return "Raw";
        case FixedStringTextRepresentation::Hex: return "Hex";
        case FixedStringTextRepresentation::Base64: return "Base64";
        case FixedStringTextRepresentation::Base64URL: return "Base64URL";
        case FixedStringTextRepresentation::Base58: return "Base58";
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown FixedString text representation");
}

FixedStringTextRepresentation parseFixedStringTextRepresentation(const String & representation)
{
    if (representation == "Raw" || representation == "raw" || representation == "RAW")
        return FixedStringTextRepresentation::Raw;
    if (representation == "Hex" || representation == "hex" || representation == "HEX")
        return FixedStringTextRepresentation::Hex;
    if (representation == "Base64" || representation == "base64" || representation == "BASE64")
        return FixedStringTextRepresentation::Base64;
    if (representation == "Base64URL" || representation == "base64url" || representation == "BASE64URL" || representation == "Base64Url")
        return FixedStringTextRepresentation::Base64URL;
    if (representation == "Base58" || representation == "base58" || representation == "BASE58")
        return FixedStringTextRepresentation::Base58;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown FixedString text representation '{}'. Supported values are Raw, Hex, Base64, Base64URL, Base58", representation);
}

DataTypeFixedString::DataTypeFixedString(size_t n_, FixedStringTextRepresentation text_representation_) : n(n_), text_representation(text_representation_)
{
    if (n == 0)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "FixedString size must be positive");
    if (n > MAX_FIXEDSTRING_SIZE)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "FixedString size is too large");
}

std::string DataTypeFixedString::doGetName() const
{
    if (text_representation == FixedStringTextRepresentation::Raw)
        return "FixedString(" + toString(n) + ")";

    return "FixedString(" + toString(n) + ", '" + fixedStringTextRepresentationToString(text_representation) + "')";
}

MutableColumnPtr DataTypeFixedString::createColumn() const
{
    return ColumnFixedString::create(n);
}

Field DataTypeFixedString::getDefault() const
{
    return String();
}

bool DataTypeFixedString::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this)
        && n == static_cast<const DataTypeFixedString &>(rhs).n
        && text_representation == static_cast<const DataTypeFixedString &>(rhs).text_representation;
}

void DataTypeFixedString::updateHashImpl(SipHash & hash) const
{
    hash.update(n);
    hash.update(static_cast<UInt8>(text_representation));
}

SerializationPtr DataTypeFixedString::doGetSerialization(const SerializationInfoSettings &) const
{
    if (text_representation == FixedStringTextRepresentation::Raw)
        return SerializationFixedString::create(n);

    return SerializationFixedStringWithTextRepresentation::create(text_representation, n);
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || (arguments->children.size() != 1 && arguments->children.size() != 2))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "FixedString data type family must have one or two arguments: size in bytes and optional text representation");

    const auto * argument = arguments->children[0]->as<ASTLiteral>();
    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                        "FixedString data type family must have a number (positive integer) as its first argument");

    FixedStringTextRepresentation text_representation = FixedStringTextRepresentation::Raw;
    if (arguments->children.size() == 2)
    {
        const auto * representation_argument = arguments->children[1]->as<ASTLiteral>();
        if (!representation_argument || representation_argument->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                            "The second argument of FixedString data type family must be a string literal with text representation");

        text_representation = parseFixedStringTextRepresentation(representation_argument->value.safeGet<String>());
    }

    return std::make_shared<DataTypeFixedString>(argument->value.safeGet<UInt64>(), text_representation);
}


void registerDataTypeFixedString(DataTypeFactory & factory)
{
    factory.registerDataType("FixedString", create);

    /// Compatibility alias.
    factory.registerAlias("BINARY", "FixedString", DataTypeFactory::Case::Insensitive);
}

}
