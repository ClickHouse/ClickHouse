#include <DataTypes/Serializations/SerializationInfoNullable.h>

#include <Columns/ColumnNullable.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationInfoObject.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>

namespace DB
{

SerializationInfoNullable::SerializationInfoNullable(MutableSerializationInfoPtr nested_, const Settings & settings_)
    : SerializationInfo({ISerialization::Kind::DEFAULT}, settings_)
    , nested(std::move(nested_))
{
    syncData();
}

bool SerializationInfoNullable::hasCustomSerialization() const
{
    return SerializationInfo::hasCustomSerialization() || nested->hasCustomSerialization();
}

bool SerializationInfoNullable::structureEquals(const SerializationInfo & rhs) const
{
    const auto * rhs_nullable = typeid_cast<const SerializationInfoNullable *>(&rhs);
    return rhs_nullable && nested->structureEquals(*rhs_nullable->nested);
}

void SerializationInfoNullable::add(const IColumn & column)
{
    nested->add(assert_cast<const ColumnNullable &>(column).getNestedColumn());
    syncData();
}

void SerializationInfoNullable::add(const SerializationInfo & other)
{
    nested->add(*assert_cast<const SerializationInfoNullable &>(other).nested);
    syncData();
}

void SerializationInfoNullable::remove(const SerializationInfo & other)
{
    nested->remove(*assert_cast<const SerializationInfoNullable &>(other).nested);
    syncData();
}

void SerializationInfoNullable::addDefaults(size_t length)
{
    nested->addDefaults(length);
    syncData();
}

void SerializationInfoNullable::replaceData(const SerializationInfo & other)
{
    nested->replaceData(*assert_cast<const SerializationInfoNullable &>(other).nested);
    syncData();
}

MutableSerializationInfoPtr SerializationInfoNullable::clone() const
{
    auto result = std::make_shared<SerializationInfoNullable>(nested->clone(), settings);
    result->setKindStack(kind_stack);
    return result;
}

MutableSerializationInfoPtr SerializationInfoNullable::createWithType(
    const IDataType & old_type,
    const IDataType & new_type,
    const Settings & new_settings) const
{
    const auto & old_nullable = assert_cast<const DataTypeNullable &>(old_type);
    const auto & new_nullable = assert_cast<const DataTypeNullable &>(new_type);
    auto result = std::make_shared<SerializationInfoNullable>(
        nested->createWithType(*old_nullable.getNestedType(), *new_nullable.getNestedType(), new_settings), new_settings);
    result->setKindStack(kind_stack);
    return result;
}

void SerializationInfoNullable::serialializeKindStackBinary(WriteBuffer & out) const
{
    SerializationInfo::serialializeKindStackBinary(out);
    nested->serialializeKindStackBinary(out);
}

void SerializationInfoNullable::deserializeFromKindsBinary(ReadBuffer & in)
{
    SerializationInfo::deserializeFromKindsBinary(in);
    nested->deserializeFromKindsBinary(in);
    syncData();
}

void SerializationInfoNullable::writeJSON(WriteBuffer & out, const String * name) const
{
    nested->writeJSON(out, name);
}

void SerializationInfoNullable::toJSON(Poco::JSON::Object & object) const
{
    nested->toJSON(object);
}

void SerializationInfoNullable::fromJSON(const Poco::JSON::Object & object)
{
    nested->fromJSON(object);
    syncData();
}

void SerializationInfoNullable::syncData()
{
    data = nested->getData();
}

bool canReuseSerializationInfoForTypeChange(const SerializationInfo & old_info, const SerializationInfo & new_info)
{
    return old_info.structureEquals(new_info)
        || (typeid_cast<const SerializationInfoObject *>(&old_info) && typeid_cast<const SerializationInfoObject *>(&new_info))
        || (typeid_cast<const SerializationInfoTuple *>(&old_info) && typeid_cast<const SerializationInfoTuple *>(&new_info));
}

MutableSerializationInfoPtr tryReuseSerializationInfoThroughNullable(
    const SerializationInfo & old_info,
    const IDataType & old_type,
    const MutableSerializationInfoPtr & new_info,
    const IDataType & new_type,
    const SerializationInfoSettings & new_settings)
{
    const auto * old_nullable_type = typeid_cast<const DataTypeNullable *>(&old_type);
    const auto * new_nullable_type = typeid_cast<const DataTypeNullable *>(&new_type);
    if (static_cast<bool>(old_nullable_type) == static_cast<bool>(new_nullable_type))
        return nullptr;

    const SerializationInfo * old_nested_info = &old_info;
    const IDataType * old_nested_type = &old_type;
    if (old_nullable_type)
    {
        const auto * old_nullable_info = typeid_cast<const SerializationInfoNullable *>(&old_info);
        if (!old_nullable_info)
            return nullptr;
        old_nested_info = old_nullable_info->getNestedInfo().get();
        old_nested_type = old_nullable_type->getNestedType().get();
    }

    const SerializationInfo * new_nested_info = new_info.get();
    const IDataType * new_nested_type = &new_type;
    if (new_nullable_type)
    {
        const auto * new_nullable_info = typeid_cast<const SerializationInfoNullable *>(new_info.get());
        if (!new_nullable_info)
            return nullptr;
        new_nested_info = new_nullable_info->getNestedInfo().get();
        new_nested_type = new_nullable_type->getNestedType().get();
    }

    if (!canReuseSerializationInfoForTypeChange(*old_nested_info, *new_nested_info))
        return nullptr;

    auto result = old_nested_info->createWithType(*old_nested_type, *new_nested_type, new_settings);
    if (new_nullable_type)
        result = std::make_shared<SerializationInfoNullable>(std::move(result), new_settings);
    return result;
}

}
