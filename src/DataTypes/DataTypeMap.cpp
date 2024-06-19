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
#include <DataTypes/Serializations/SerializationInfoMap.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromMemory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

DataTypeMap::DataTypeMap(const DataTypePtr & nested_) : nested(nested_)
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
    : key_type(key_type_)
    , value_type(value_type_)
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

MutableColumnPtr DataTypeMap::createColumn(const ISerialization & serialization) const
{
    const auto * nested_serialization = removeWrapper<SerializationNamed>(serialization);
    size_t num_shards = assert_cast<const SerializationMap &>(*nested_serialization).getNumShards();

    MutableColumns shards(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
        shards[i] = nested->createColumn();

    return ColumnMap::create(std::move(shards));
}

Field DataTypeMap::getDefault() const
{
    return Map();
}

SerializationPtr DataTypeMap::getSerializationWithShards(size_t num_shards) const
{
    return std::make_shared<SerializationMap>(key_type->getDefaultSerialization(), value_type->getDefaultSerialization(), nested->getDefaultSerialization(), num_shards);
}

SerializationPtr DataTypeMap::doGetDefaultSerialization() const
{
    return getSerializationWithShards(1);
}

MutableSerializationInfoPtr DataTypeMap::createSerializationInfo(const SerializationInfoSettings & settings) const
{
    return std::make_shared<SerializationInfoMap>(settings.type_map_num_shards, ISerialization::Kind::DEFAULT, settings);
}

SerializationInfoPtr DataTypeMap::getSerializationInfo(const IColumn &) const
{
    return std::make_shared<SerializationInfoMap>(/*num_shards=*/ 1, ISerialization::Kind::DEFAULT, SerializationInfo::Settings{});
}

SerializationPtr DataTypeMap::getSerialization(const SerializationInfo & info) const
{
    return getSerializationWithShards(assert_cast<const SerializationInfoMap &>(info).getNumShards());
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

std::unique_ptr<IDataType::SubstreamData> DataTypeMap::getDynamicSubcolumnData(
    std::string_view subcolumn_name,
    const SubstreamData & data,
    bool throw_if_null) const
{
    auto throw_or_return = [&]
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());
        return nullptr;
    };

    static constexpr std::string_view shard_prefix = "shard_";

    if (data.serialization && subcolumn_name.starts_with(shard_prefix))
    {
        size_t num_shards = assert_cast<const SerializationMap &>(*data.serialization).getNumShards();

        std::string_view shard_str = subcolumn_name.substr(shard_prefix.size());
        ReadBufferFromMemory buf(shard_str.data(), shard_str.size());

        UInt32 shard_hash;
        if (!tryReadIntText(shard_hash, buf) || !buf.eof())
            return throw_or_return();

        if (num_shards == 1)
            return std::make_unique<SubstreamData>(data);

        UInt32 shard = shard_hash % num_shards;
        String shard_subcolumn = String(shard_prefix) + toString(shard);

        SubstreamData shard_data = data;

        shard_data.serialization = std::make_shared<SerializationNamed>(
            getSerializationWithShards(1),
            shard_subcolumn,
            ISerialization::Substream::MapShard);

        return std::make_unique<SubstreamData>(std::move(shard_data));
    }

    return throw_or_return();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Map data type family must have 2 arguments: key and value types");

    const auto & children = arguments->children;
    DataTypes nested_types
    {
        DataTypeFactory::instance().get(children[0]),
        DataTypeFactory::instance().get(children[1]),
    };

    return std::make_shared<DataTypeMap>(nested_types);
}


void registerDataTypeMap(DataTypeFactory & factory)
{
    factory.registerDataType("Map", create);
}
}
