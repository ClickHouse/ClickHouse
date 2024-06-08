#include <base/map.h>
#include <Common/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

DataTypeMap::DataTypeMap(const DataTypePtr & nested_, size_t num_shards_)
    : nested(nested_), num_shards(num_shards_)
{
    const auto * type_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!type_array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
    if (!type_tuple)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    if (type_tuple->getElements().size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    key_type = type_tuple->getElement(0);
    value_type = type_tuple->getElement(1);
    assertKeyType();
}

DataTypeMap::DataTypeMap(const DataTypes & elems_, size_t num_shards_)
    : num_shards(num_shards_)
{
    assert(elems_.size() == 2);
    key_type = elems_[0];
    value_type = elems_[1];

    assertKeyType();

    nested = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type, value_type}, Names{"keys", "values"}));
}

DataTypeMap::DataTypeMap(const DataTypePtr & key_type_, const DataTypePtr & value_type_, size_t num_shards_)
    : key_type(key_type_), value_type(value_type_)
    , nested(std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type_, value_type_}, Names{"keys", "values"})))
    , num_shards(num_shards_)
{
    assertKeyType();
}

void DataTypeMap::assertKeyType() const
{
    if (!isValidKeyType(key_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Map cannot have a key of type {}", key_type->getName());
}


std::string DataTypeMap::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "Map(" << key_type->getName() << ", " << value_type->getName();
    if (num_shards != DEFAULT_NUMBER_OF_SHARDS)
        s << ", " << num_shards;
    s << ")";

    return s.str();
}

std::string DataTypeMap::doGetPrettyName(size_t indent) const
{
    WriteBufferFromOwnString s;
    s << "Map(" << key_type->getPrettyName(indent) << ", " << value_type->getPrettyName(indent) << ')';
    return s.str();
}

MutableColumnPtr DataTypeMap::createColumn() const
{
    return ColumnMap::create(nested->createColumn());
}

Field DataTypeMap::getDefault() const
{
    return Map();
}

SerializationPtr DataTypeMap::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationMap>(
        key_type->getDefaultSerialization(),
        value_type->getDefaultSerialization(),
        nested->getDefaultSerialization(),
        num_shards);
}

bool DataTypeMap::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeMap & rhs_map = static_cast<const DataTypeMap &>(rhs);
    return nested->equals(*rhs_map.nested);
}

bool DataTypeMap::isValidKeyType(DataTypePtr key_type)
{
    return !isNullableOrLowCardinalityNullable(key_type);
}

DataTypePtr DataTypeMap::getNestedTypeWithUnnamedTuple() const
{
    const auto & from_array = assert_cast<const DataTypeArray &>(*nested);
    const auto & from_tuple = assert_cast<const DataTypeTuple &>(*from_array.getNestedType());
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(from_tuple.getElements()));
}

void DataTypeMap::forEachChild(const DB::IDataType::ChildCallback & callback) const
{
    callback(*key_type);
    key_type->forEachChild(callback);
    callback(*value_type);
    value_type->forEachChild(callback);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() < 2 || arguments->children.size() > 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Map data type family must have 2-3 arguments: key type, value type and optional number of shards");

    const auto & children = arguments->children;
    DataTypes nested_types
    {
        DataTypeFactory::instance().get(children[0]),
        DataTypeFactory::instance().get(children[1]),
    };

    size_t num_shards = DataTypeMap::DEFAULT_NUMBER_OF_SHARDS;
    if (arguments->children.size() == 3)
    {
        const auto * literal = arguments->children[2]->as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::UInt64 || literal->value.get<UInt64>() == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Third argument for data type Map must be positive integer literal");

        num_shards = literal->value.get<UInt64>();
    }

    return std::make_shared<DataTypeMap>(nested_types, num_shards);
}


void registerDataTypeMap(DataTypeFactory & factory)
{
    factory.registerDataType("Map", create);
}
}
