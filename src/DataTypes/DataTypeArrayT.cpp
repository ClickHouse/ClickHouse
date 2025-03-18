#include <Columns/ColumnArrayT.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeArrayT.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationArrayT.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <base/map.h>
#include <base/range.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int LOGICAL_ERROR;
extern const int UNEXPECTED_AST_STRUCTURE;
}

DataTypeArrayT::DataTypeArrayT(const DataTypePtr & type_, const size_t size_, const size_t n_)
    : type(type_)
    , size(size_)
    , n(n_)
{
}

std::string DataTypeArrayT::doGetName() const
{
    return "ArrayT(" + type->getName() + ", " + toString(n) + ")";
}

/// This is called when the first value is added, not when table is created
MutableColumnPtr DataTypeArrayT::createColumn() const
{
    MutableColumns tuple_columns(size);

    for (size_t i = 0; i < size; ++i)
        tuple_columns[i] = ColumnFixedString::create(n >> 3); // As n is bits and we need bytes

    MutableColumnPtr tuple_column = ColumnTuple::create(std::move(tuple_columns));
    return ColumnArrayT::create(std::move(tuple_column));
}

/// TODO: the following methods until create() are placeholders and need to be implemented
void DataTypeArrayT::insertDefaultInto(IColumn & column) const
{
    if (!type)
    {
        column.insertDefault();
        return;
    }
}

bool DataTypeArrayT::equals(const IDataType & rhs) const
{
    return rhs.getSizeOfValueInMemory() != 0;
}

SerializationPtr DataTypeArrayT::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationArrayT>(size, n);
}

Field DataTypeArrayT::getDefault() const
{
    return 0;
}

size_t DataTypeArrayT::getSizeOfValueInMemory() const
{
    return type->getSizeOfValueInMemory() * n;
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "ArrayT data type family must have exactly two argument: type of vector elements and their number");

    const DataTypePtr type = DataTypeFactory::instance().get(arguments->children[0]);
    const auto * argument = arguments->children[1]->as<ASTLiteral>();

    if (type->getTypeId() != TypeIndex::BFloat16 && type->getTypeId() != TypeIndex::Float32 && type->getTypeId() != TypeIndex::Float64)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "ArrayT data type only supports BFloat16, Float32, or Float64 as element type");

    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.safeGet<UInt64>() == 0)
        throw Exception(
            ErrorCodes::UNEXPECTED_AST_STRUCTURE, "ArrayT data type must have a number (positive integer) as its second argument");

    const size_t size = type->getTypeId() == TypeIndex::BFloat16 ? 16
        : type->getTypeId() == TypeIndex::Float32                ? 32
        : type->getTypeId() == TypeIndex::Float64
        ? 64
        : throw Exception(ErrorCodes::BAD_ARGUMENTS, "ArrayT data type only supports BFloat16, Float32, or Float64");

    auto temp = std::make_shared<DataTypeArrayT>(type, size, argument->value.safeGet<UInt64>());

    return std::move(temp);
}


void registerDataTypeArrayT(DataTypeFactory & factory)
{
    factory.registerDataType("ArrayT", create);
}

}
