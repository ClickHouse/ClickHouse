#include <DataTypes/Serializations/SerializationNullableElements.h>

#include <Common/SipHash.h>

namespace DB
{

SerializationNullableElements::SerializationNullableElements(const SerializationPtr & nested_)
    : SerializationWrapper(nested_)
{
}

UInt128 SerializationNullableElements::getHash(const SerializationPtr & nested_)
{
    SipHash hash;
    hash.update("NullableElements");
    hash.update(nested_->getHash());
    return hash.get128();
}

SerializationPtr SerializationNullableElements::create(const SerializationPtr & nested_)
{
    if (!nested_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationNullableElements(nested_));
    return ISerialization::pooled(getHash(nested_), [&] { return new SerializationNullableElements(nested_); });
}

void SerializationNullableElements::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    settings.path.push_back(Substream::NullableElements);
    settings.path.back().data = data;
    nested_serialization->enumerateStreams(settings, callback, data);
    settings.path.pop_back();
}

void SerializationNullableElements::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::NullableElements);
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state, cache);
    settings.path.pop_back();
}

void SerializationNullableElements::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::NullableElements);
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
    settings.path.pop_back();
}

}
