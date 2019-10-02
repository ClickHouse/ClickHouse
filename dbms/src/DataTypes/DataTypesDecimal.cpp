#include <DataTypes/DataTypesDecimal.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/readDecimalText.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>

#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

//

template <typename T>
std::string DataTypeDecimal<T>::doGetName() const
{
    std::stringstream ss;
    ss << "Decimal(" << this->precision << ", " << this->scale << ")";
    return ss.str();
}

template <typename T>
DataTypePtr DataTypeDecimal<T>::promoteNumericType() const
{
    using PromotedType = DataTypeDecimal<Decimal128>;
    return std::make_shared<PromotedType>(PromotedType::maxPrecision(), this->scale);
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("Decimal data type family must have exactly two arguments: precision and scale",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * precision = arguments->children[0]->as<ASTLiteral>();
    const auto * scale = arguments->children[1]->as<ASTLiteral>();

    if (!precision || precision->value.getType() != Field::Types::UInt64 ||
        !scale || !(scale->value.getType() == Field::Types::Int64 || scale->value.getType() == Field::Types::UInt64))
        throw Exception("Decimal data type family must have a two numbers as its arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    UInt64 precision_value = precision->value.get<UInt64>();
    UInt64 scale_value = scale->value.get<UInt64>();

    return createDecimal<DataTypeDecimal>(precision_value, scale_value);
}

template <typename T>
static DataTypePtr createExact(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("Decimal data type family must have exactly two arguments: precision and scale",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * scale_arg = arguments->children[0]->as<ASTLiteral>();

    if (!scale_arg || !(scale_arg->value.getType() == Field::Types::Int64 || scale_arg->value.getType() == Field::Types::UInt64))
        throw Exception("Decimal data type family must have a two numbers as its arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    UInt64 precision = maxDecimalPrecision<T>();
    UInt64 scale = scale_arg->value.get<UInt64>();

    return createDecimal<DataTypeDecimal>(precision, scale);
}

void registerDataTypeDecimal(DataTypeFactory & factory)
{
    factory.registerDataType("Decimal32", createExact<Decimal32>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("Decimal64", createExact<Decimal64>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("Decimal128", createExact<Decimal128>, DataTypeFactory::CaseInsensitive);

    factory.registerDataType("Decimal", create, DataTypeFactory::CaseInsensitive);
    factory.registerAlias("DEC", "Decimal", DataTypeFactory::CaseInsensitive);
}

/// Explicit template instantiations.
template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128>;

}
