#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationObjectTypedPath.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


UInt128 SerializationObjectTypedPath::getHash(const SerializationPtr & nested_, const String & path_)
{
    SipHash hash;
    hash.update("ObjectTypedPath");
    hash.update(nested_->getHash());
    hash.update(path_.size());
    hash.update(path_);
    return hash.get128();
}

SerializationPtr SerializationObjectTypedPath::create(const SerializationPtr & nested_, const String & path_)
{
    if (!nested_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationObjectTypedPath(nested_, path_));
    return ISerialization::pooled(getHash(nested_, path_), [&] { return new SerializationObjectTypedPath(nested_, path_); });
}

void SerializationObjectTypedPath::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedPath);
    settings.path.back().object_path_name = path;
    auto path_data = SubstreamData(nested_serialization)
                         .withType(data.type)
                         .withColumn(data.column)
                         .withSerializationInfo(data.serialization_info)
                         .withDeserializeState(data.deserialize_state);
    nested_serialization->enumerateStreams(settings, callback, path_data);
    settings.path.pop_back();
    settings.path.pop_back();
}

void SerializationObjectTypedPath::serializeBinaryBulkStatePrefix(const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationObjectTypedPath");
}

void SerializationObjectTypedPath::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationObjectTypedPath");
}

void SerializationObjectTypedPath::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedPath);
    settings.path.back().object_path_name = path;
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state, cache);
    settings.path.pop_back();
    settings.path.pop_back();
}

void SerializationObjectTypedPath::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationObjectTypedPath");
}

void SerializationObjectTypedPath::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & result_column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedPath);
    settings.path.back().object_path_name = path;
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(result_column, rows_offset, limit, settings, state, cache);
    settings.path.pop_back();
    settings.path.pop_back();
}

size_t SerializationObjectTypedPath::allocatedBytes() const
{
    return sizeof(*this) + path.capacity();
}

}
