#include <Common/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <Parsers/IAST.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

DataTypeMap::DataTypeMap(const DataTypePtr & nested_)
    : nested(nested_)
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

    if (type_tuple->hasExplicitNames())
    {
        const auto & names = type_tuple->getElementNames();
        if (names[0] != "keys" || names[1] != "values")
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected Tuple(key, value) with explicit names 'keys', 'values', got explicit names '{}', '{}'", names[0], names[1]);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Tuple(key, value) with explicit names 'keys', 'values', got without explicit names");

    key_type = type_tuple->getElement(0);
    value_type = type_tuple->getElement(1);
    assertKeyType();
}

DataTypeMap::DataTypeMap(const DataTypes & elems_)
{
    assert(elems_.size() == 2);
    key_type = elems_[0];
    value_type = elems_[1];

    assertKeyType();

    nested = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type, value_type}, Names{"keys", "values"}));
}

DataTypeMap::DataTypeMap(const DataTypePtr & key_type_, const DataTypePtr & value_type_)
    : key_type(key_type_), value_type(value_type_)
    , nested(std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type_, value_type_}, Names{"keys", "values"})))
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
    s << "Map(" << key_type->getName() << ", " << value_type->getName() << ")";

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
    auto key_serialization = key_type->getDefaultSerialization();
    auto value_serialization = value_type->getDefaultSerialization();
    /// Don't use nested->getDefaultSerialization() to avoid creating exponentially growing number of serializations for deep nested maps.
    /// Instead, reuse already created serializations for keys and values.
    auto key_serialization_named = std::make_shared<SerializationNamed>(key_serialization, "keys", SubstreamType::TupleElement);
    auto value_serialization_named = std::make_shared<SerializationNamed>(value_serialization, "values", SubstreamType::TupleElement);
    auto nested_serialization = std::make_shared<SerializationArray>(std::make_shared<SerializationTuple>(SerializationTuple::ElementSerializations{key_serialization_named, value_serialization_named}, true));
    return std::make_shared<SerializationMap>(key_serialization, value_serialization, nested_serialization);
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

void DataTypeMap::updateHashImpl(SipHash & hash) const
{
    key_type->updateHash(hash);
    value_type->updateHash(hash);
}

void DataTypeMap::forEachChild(const DB::IDataType::ChildCallback & callback) const
{
    callback(*key_type);
    callback(*value_type);
    key_type->forEachChild(callback);
    value_type->forEachChild(callback);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Map data type family must have two arguments: key and value types");

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
        nested_types.emplace_back(DataTypeFactory::instance().get(child));

    return std::make_shared<DataTypeMap>(nested_types);
}


void registerDataTypeMap(DataTypeFactory & factory)
{
    factory.registerDataType("Map", create);
}
}
