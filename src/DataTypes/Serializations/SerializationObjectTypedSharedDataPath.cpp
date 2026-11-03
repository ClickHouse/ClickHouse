#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationObjectTypedSharedDataPath.h>
#include <Common/SipHash.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

SerializationObjectTypedSharedDataPath::SerializationObjectTypedSharedDataPath(
    const SerializationPtr & nested_,
    const SerializationObjectTypedSharedData::SharedTypedPath & path_,
    size_t path_index_,
    size_t num_paths_,
    const String & subcolumn_name_)
    : SerializationWrapper(nested_)
    , path(path_)
    , path_index(path_index_)
    , num_paths(num_paths_)
    , subcolumn_name(subcolumn_name_)
{
}

UInt128 SerializationObjectTypedSharedDataPath::getHash(
    const SerializationPtr & nested_,
    const SerializationObjectTypedSharedData::SharedTypedPath & path_,
    size_t path_index_,
    size_t num_paths_,
    const String & subcolumn_name_)
{
    SipHash hash;
    hash.update("ObjectTypedSharedDataPath");
    hash.update(nested_->getHash());
    hash.update(path_.name.size());
    hash.update(path_.name);
    auto type_name = path_.type->getName();
    hash.update(type_name.size());
    hash.update(type_name);
    hash.update(path_.serialization->getHash());
    hash.update(path_index_);
    hash.update(num_paths_);
    hash.update(subcolumn_name_.size());
    hash.update(subcolumn_name_);
    return hash.get128();
}

SerializationPtr SerializationObjectTypedSharedDataPath::create(
    const SerializationPtr & nested_,
    const SerializationObjectTypedSharedData::SharedTypedPath & path_,
    size_t path_index_,
    size_t num_paths_,
    const String & subcolumn_name_)
{
    if (!nested_->supportsPooling() || !path_.serialization->supportsPooling())
        return std::shared_ptr<ISerialization>(
            new SerializationObjectTypedSharedDataPath(nested_, path_, path_index_, num_paths_, subcolumn_name_));
    return ISerialization::pooled(
        getHash(nested_, path_, path_index_, num_paths_, subcolumn_name_),
        [&] { return new SerializationObjectTypedSharedDataPath(nested_, path_, path_index_, num_paths_, subcolumn_name_); });
}

size_t SerializationObjectTypedSharedDataPath::allocatedBytes() const
{
    return sizeof(*this) + path.name.capacity() + subcolumn_name.capacity();
}

void SerializationObjectTypedSharedDataPath::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedSharedData);
    /// The identity element keys the path's cache entries.
    settings.path.push_back(SerializationObjectTypedSharedData::getPathElement(path.name));
    SCOPE_EXIT({
        settings.path.pop_back();
        settings.path.pop_back();
        settings.path.pop_back();
    });

    /// With a reader's state at hand, the path's bytes of the shared data stream, for the part's subcolumn sizes.
    SerializationObjectTypedSharedData::enumerateTypedSharedDataStreams(
        settings, callback, SerializationObjectTypedSharedData::pathsSizeHint(data.deserialize_state, path_index, 1));
}

void SerializationObjectTypedSharedDataPath::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationObjectTypedSharedDataPath");
}

void SerializationObjectTypedSharedDataPath::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationObjectTypedSharedDataPath");
}

void SerializationObjectTypedSharedDataPath::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationObjectTypedSharedDataPath");
}

void SerializationObjectTypedSharedDataPath::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    if (settings.data_part_type == MergeTreeDataPartType::Unknown)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shared typed path {} of Object is read outside a data part", path.name);

    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedSharedData);
    SCOPE_EXIT({
        settings.path.pop_back();
        settings.path.pop_back();
    });

    auto structure_state = SerializationObjectTypedSharedData::deserializeStructureStatePrefix(settings, cache, num_paths);
    if (!structure_state)
        return;

    /// A subcolumn reader registers the whole path too: it is extracted from the decoded path.
    auto * structure
        = checkAndGetState<SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedDataStructure>(structure_state);
    structure->requestPath(path_index, path);
    state = std::move(structure_state);
}

void SerializationObjectTypedSharedDataPath::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    /// The column is absent from the part: the prefix found no header, and the reader fills the defaults.
    if (!state)
        return;

    auto * structure
        = checkAndGetState<SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedDataStructure>(state);

    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedSharedData);
    SCOPE_EXIT({
        settings.path.pop_back();
        settings.path.pop_back();
    });

    if (subcolumn_name.empty())
    {
        if (SerializationObjectTypedSharedData::insertPathFromCache(column, path.name, settings, cache))
            return;
        SerializationObjectTypedSharedData::deserializeRequestedPaths(
            limit, settings, *structure, cache, [&](size_t index) -> IColumn * { return index == path_index ? &column : nullptr; });
        return;
    }

    /// The subcolumn comes out of the path's column of the call, decoded by this reader if no other did.
    settings.path.push_back(SerializationObjectTypedSharedData::getPathElement(path.name));
    auto cached = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path);
    settings.path.pop_back();
    if (!cached)
    {
        SerializationObjectTypedSharedData::deserializeRequestedPaths(limit, settings, *structure, cache, [](size_t) -> IColumn * { return nullptr; });
        settings.path.push_back(SerializationObjectTypedSharedData::getPathElement(path.name));
        cached = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path);
        settings.path.pop_back();
        if (!cached)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object is not in the substreams cache", path.name);
    }

    const auto & [path_column, num_read_rows] = *cached;
    auto subcolumn = path.type->getSubcolumn(subcolumn_name, path_column);
    column.insertRangeFrom(*subcolumn, subcolumn->size() - num_read_rows, num_read_rows);
}

}
