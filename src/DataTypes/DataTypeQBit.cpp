#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationFixedString.h>
#include <DataTypes/Serializations/SerializationQBit.h>
#include <Parsers/ASTLiteral.h>

#include <base/range.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UNEXPECTED_AST_STRUCTURE;
}

DataTypeQBit::DataTypeQBit(const DataTypePtr & element_type_, const size_t dimension_)
    : element_type(element_type_)
    , dimension(dimension_)
{
}

std::string DataTypeQBit::doGetName() const
{
    return "QBit(" + element_type->getName() + ", " + toString(dimension) + ")";
}

/// This is called when values are added, not on CREATE TABLE
MutableColumnPtr DataTypeQBit::createColumn() const
{
    /// Continue with column creation
    MutableColumns tuple_columns(getElementSize());
    size_t bytes = bitsToBytes(dimension);

    for (size_t i = 0; i < getElementSize(); ++i)
        tuple_columns[i] = ColumnFixedString::create(bytes);

    return ColumnQBit::create(IColumn::mutate(ColumnTuple::create(std::move(tuple_columns))), dimension);
}

bool DataTypeQBit::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeQBit & rhsq = static_cast<const DataTypeQBit &>(rhs);

    return element_type.get()->equals(*rhsq.element_type.get()) && getElementSize() == rhsq.getElementSize() && dimension == rhsq.dimension;
}

DataTypePtr DataTypeQBit::getNestedType() const
{
    auto fixed_string_type = getNestedTupleElementType();
    DataTypes tuple_element_types(getElementSize(), fixed_string_type);
    return std::make_shared<DataTypeTuple>(tuple_element_types);
}

DataTypePtr DataTypeQBit::getNestedTupleElementType() const
{
    return std::make_shared<DataTypeFixedString>(bitsToBytes(dimension));
}

SerializationPtr DataTypeQBit::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationQBit>(getNestedType()->getDefaultSerialization(), getElementSize(), dimension);
}

Field DataTypeQBit::getDefault() const
{
    return getNestedType()->getDefault();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    /// Check if arguments are valid
    if (!arguments || arguments->children.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "QBit data type family must have exactly two argument: type of vector elements and their number");

    const DataTypePtr type = DataTypeFactory::instance().get(arguments->children[0]);
    const auto * argument = arguments->children[1]->as<ASTLiteral>();

    if (type->getTypeId() != TypeIndex::BFloat16 && type->getTypeId() != TypeIndex::Float32 && type->getTypeId() != TypeIndex::Float64)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "QBit data type only supports BFloat16, Float32, or Float64 as element type. Got: {}",
            type->getName());

    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.safeGet<UInt64>() == 0)
        throw Exception(
            ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "QBit data type must have a number (positive integer) as its second argument. Got: {}",
            arguments->children[1]->formatForErrorMessage());

    return std::make_shared<DataTypeQBit>(type, argument->value.safeGet<UInt64>());
}


void registerDataTypeQBit(DataTypeFactory & factory)
{
    factory.registerDataType("QBit", create);
}

}
