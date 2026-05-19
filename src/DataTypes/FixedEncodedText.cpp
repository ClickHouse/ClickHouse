#include <DataTypes/FixedEncodedText.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/Serializations/SerializationFixedEncodedText.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

String getFixedEncodedTextTypeName(FixedEncodedTextKind kind, size_t n)
{
    return String(kind == FixedEncodedTextKind::Base58 ? "FixedBase58" : "FixedBase64") + "(" + toString(n) + ")";
}

namespace
{

class DataTypeFixedEncodedTextName final : public IDataTypeCustomName
{
public:
    DataTypeFixedEncodedTextName(FixedEncodedTextKind kind_, size_t n_) : kind(kind_), n(n_) {}

    String getName() const override { return getFixedEncodedTextTypeName(kind, n); }

private:
    FixedEncodedTextKind kind;
    size_t n;
};

size_t parseFixedEncodedTextSize(const ASTPtr & arguments, const char * type_name)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "{} data type family must have exactly one argument - size in decoded bytes", type_name);

    const auto * argument = arguments->children[0]->as<ASTLiteral>();
    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "{} data type family must have a number (positive integer) as its argument", type_name);

    size_t n = argument->value.safeGet<UInt64>();
    if (n > MAX_FIXEDSTRING_SIZE)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "{} size is too large", type_name);

    return n;
}

std::pair<DataTypePtr, DataTypeCustomDescPtr> create(FixedEncodedTextKind kind, const ASTPtr & arguments)
{
    const char * type_name = kind == FixedEncodedTextKind::Base58 ? "FixedBase58" : "FixedBase64";
    const size_t n = parseFixedEncodedTextSize(arguments, type_name);

    auto type = std::make_shared<DataTypeFixedString>(n);
    auto desc = std::make_unique<DataTypeCustomDesc>(
        std::make_unique<DataTypeFixedEncodedTextName>(kind, n),
        SerializationFixedEncodedText::create(kind, n));

    return {std::move(type), std::move(desc)};
}

}

std::optional<FixedEncodedTextInfo> getFixedEncodedTextInfo(const IDataType & type)
{
    const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(&type);
    if (!fixed_string || !type.getCustomName() || !type.getCustomSerialization())
        return std::nullopt;

    const String name = type.getCustomName()->getName();
    if (name == getFixedEncodedTextTypeName(FixedEncodedTextKind::Base58, fixed_string->getN()))
        return FixedEncodedTextInfo{FixedEncodedTextKind::Base58, fixed_string->getN()};
    if (name == getFixedEncodedTextTypeName(FixedEncodedTextKind::Base64, fixed_string->getN()))
        return FixedEncodedTextInfo{FixedEncodedTextKind::Base64, fixed_string->getN()};

    return std::nullopt;
}

DataTypePtr createFixedEncodedTextType(FixedEncodedTextKind kind, size_t n)
{
    return DataTypeFactory::instance().get(String(kind == FixedEncodedTextKind::Base58 ? "FixedBase58" : "FixedBase64") + "(" + toString(n) + ")");
}

void registerDataTypeFixedEncodedText(DataTypeFactory & factory)
{
    factory.registerDataTypeCustom("FixedBase58", [](const ASTPtr & arguments) { return create(FixedEncodedTextKind::Base58, arguments); });
    factory.registerDataTypeCustom("FixedBase64", [](const ASTPtr & arguments) { return create(FixedEncodedTextKind::Base64, arguments); });
}

}
