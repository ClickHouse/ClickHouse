#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationObjectTypedPath.h>
#include <IO/ReadHelpers.h>
#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/Serializations/SerializationNamed.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace
{
struct DeserializeStateTypedPath : public ISerialization::DeserializeBinaryBulkState
{
    SerializationPtr serialization;
    ISerialization::DeserializeBinaryBulkStatePtr nested;
    bool sparse = false;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto result = std::make_shared<DeserializeStateTypedPath>(*this);
        result->nested = nested ? nested->clone() : nullptr;
        return result;
    }
};

SerializationPtr sparsePathSerialization(const SerializationPtr & nested)
{
    /// Nullable sparse paths have no physical null map. Its logical .null subcolumn
    /// must reconstruct ones for missing rows from the same SparseOffsets stream.
    if (const auto * named = typeid_cast<const SerializationNamed *>(nested.get()); named && named->getElementName() == "null")
        return SerializationSparseNullMap::create();
    return SerializationSparse::create(nested);
}
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
    settings.path.push_back(Substream::ObjectStructure);
    callback(settings.path);
    settings.path.pop_back();
    const auto * path_state = data.deserialize_state ? checkAndGetState<DeserializeStateTypedPath>(data.deserialize_state) : nullptr;
    auto serialization = path_state ? path_state->serialization : nested_serialization;
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedPath);
    settings.path.back().object_path_name = path;
    auto path_data = SubstreamData(serialization)
                         .withType(data.type)
                         .withColumn(data.column)
                         .withSerializationInfo(data.serialization_info)
                         .withDeserializeState(path_state ? path_state->nested : nullptr);
    serialization->enumerateStreams(settings, callback, path_data);
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
    auto structure = SerializationObject::deserializeObjectStructureStatePrefix(settings, cache);
    auto path_state = std::make_shared<DeserializeStateTypedPath>();
    bool sparse = structure && checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(structure)->sparse_typed_paths.contains(path);
    path_state->serialization = sparse ? sparsePathSerialization(nested_serialization) : nested_serialization;
    path_state->sparse = typeid_cast<const SerializationSparse *>(path_state->serialization.get());
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedPath);
    settings.path.back().object_path_name = path;
    path_state->serialization->deserializeBinaryBulkStatePrefix(settings, path_state->nested, cache);
    state = std::move(path_state);
    settings.path.pop_back();
    settings.path.pop_back();
}

void SerializationObjectTypedPath::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationObjectTypedPath");
}

void SerializationObjectTypedPath::deserializeBinaryBulkWithMultipleStreams(
    IColumn & result_column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedPath);
    settings.path.back().object_path_name = path;
    auto * path_state = checkAndGetState<DeserializeStateTypedPath>(state);
    if (path_state->sparse)
        SerializationObject::deserializeSparsePath(path_state->serialization, result_column, limit, settings, path_state->nested, cache);
    else
        path_state->serialization->deserializeBinaryBulkWithMultipleStreams(result_column, limit, settings, path_state->nested, cache);
    settings.path.pop_back();
    settings.path.pop_back();
}

size_t SerializationObjectTypedPath::allocatedBytes() const
{
    return sizeof(*this) + path.capacity();
}

}
