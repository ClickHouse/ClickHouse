#include <Columns/ColumnDenseVector.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVector.h>
#include <DataTypes/Serializations/SerializationVector.h>
#include <Common/SipHash.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UNEXPECTED_AST_STRUCTURE;
}

DataTypeVector::DataTypeVector(const DataTypePtr & element_type_, const size_t dimension_)
    : element_type(element_type_)
    , dimension(dimension_)
{
    /// Prevents maliciously crafted byte streams from being deserialized into illegal types.
    if (element_type_->getTypeId() != TypeIndex::BFloat16 && element_type_->getTypeId() != TypeIndex::Float32
        && element_type_->getTypeId() != TypeIndex::Float64)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Vector data type only supports BFloat16, Float32, or Float64 as element type. Got: {}",
            element_type_->getName());

    /// Like QBit, ensure that query parameters such as `SET param_v=[1,2,3]; SELECT {v:Vector(Float32,3)}` keep
    /// their original string value during substitution instead of being extracted to a Field that fails to cast back.
    custom_serialization = getDefaultSerialization();
}

std::string DataTypeVector::doGetName() const
{
    return "Vector(" + element_type->getName() + ", " + toString(dimension) + ")";
}

MutableColumnPtr DataTypeVector::createColumn() const
{
    return ColumnDenseVector::create(
        IColumn::mutate(ColumnFixedString::create(getSizeOfValueInMemory())), getElementSizeInBytes(), dimension);
}

bool DataTypeVector::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const auto & rhs_vector = static_cast<const DataTypeVector &>(rhs);
    return element_type->equals(*rhs_vector.element_type) && dimension == rhs_vector.dimension;
}

void DataTypeVector::updateHashImpl(SipHash & hash) const
{
    hash.update("Vector");
    element_type->updateHashImpl(hash);
    hash.update(dimension);
}

SerializationPtr DataTypeVector::doGetSerialization(const SerializationInfoSettings &) const
{
    return SerializationVector::create(getElementSizeInBytes(), dimension);
}

Field DataTypeVector::getDefault() const
{
    /// An empty string is aligned (zero-padded) to the fixed value size on insert, yielding a zero vector.
    return String();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Vector data type family must have exactly two arguments: type of vector elements and their number");

    const DataTypePtr type = DataTypeFactory::instance().get(arguments->children[0]);
    const auto * argument = arguments->children[1]->as<ASTLiteral>();

    if (type->getTypeId() != TypeIndex::BFloat16 && type->getTypeId() != TypeIndex::Float32 && type->getTypeId() != TypeIndex::Float64)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Vector data type only supports BFloat16, Float32, or Float64 as element type. Got: {}",
            type->getName());

    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.safeGet<UInt64>() == 0)
        throw Exception(
            ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "Vector data type must have a number (positive integer) as its second argument. Got: {}",
            arguments->children[1]->formatForErrorMessage());

    return std::make_shared<DataTypeVector>(type, argument->value.safeGet<UInt64>());
}

void registerDataTypeVector(DataTypeFactory & factory)
{
    factory.registerDataType("Vector", create);
}

}
