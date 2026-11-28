#include <Columns/ColumnObject.h>
#include <DataTypes/Serializations/SerializationSubObjectTypedSharedData.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

SerializationSubObjectTypedSharedData::SerializationSubObjectTypedSharedData(
    const String & paths_prefix_, SharedTypedPaths paths_, size_t first_path_index_, size_t num_paths_)
    : paths_prefix(paths_prefix_)
    , paths(std::move(paths_))
    , first_path_index(first_path_index_)
    , num_paths(num_paths_)
{
    if (paths.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sub-object {} of the typed shared data of Object has no paths", paths_prefix);
}

UInt128 SerializationSubObjectTypedSharedData::getHash(
    const String & paths_prefix_, const SharedTypedPaths & paths_, size_t first_path_index_, size_t num_paths_)
{
    SipHash hash;
    hash.update("SubObjectTypedSharedData");
    hash.update(paths_prefix_.size());
    hash.update(paths_prefix_);
    hash.update(SerializationObjectTypedSharedData::getHash(paths_));
    hash.update(first_path_index_);
    hash.update(num_paths_);
    return hash.get128();
}

SerializationPtr SerializationSubObjectTypedSharedData::create(
    const String & paths_prefix_, SharedTypedPaths paths_, size_t first_path_index_, size_t num_paths_)
{
    bool pooling = std::ranges::all_of(paths_, [](const auto & path) { return path.serialization->supportsPooling(); });
    if (!pooling)
        return std::shared_ptr<ISerialization>(
            new SerializationSubObjectTypedSharedData(paths_prefix_, std::move(paths_), first_path_index_, num_paths_));

    return ISerialization::pooled(
        getHash(paths_prefix_, paths_, first_path_index_, num_paths_),
        [&] { return new SerializationSubObjectTypedSharedData(paths_prefix_, std::move(paths_), first_path_index_, num_paths_); });
}

size_t SerializationSubObjectTypedSharedData::allocatedBytes() const
{
    size_t bytes = sizeof(*this) + paths_prefix.capacity() + paths.capacity() * sizeof(SharedTypedPaths::value_type);
    for (const auto & path : paths)
        bytes += path.name.capacity();
    return bytes;
}

bool SerializationSubObjectTypedSharedData::supportsPooling() const
{
    return std::ranges::all_of(paths, [](const auto & path) { return path.serialization->supportsPooling(); });
}

void SerializationSubObjectTypedSharedData::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    /// The caller has pushed ObjectData and ObjectTypedSharedData. Each path is listed behind its identity element,
    /// with its bytes of the data stream when a state is at hand.
    for (size_t i = 0; i != paths.size(); ++i)
    {
        settings.path.push_back(SerializationObjectTypedSharedData::getPathElement(paths[i].name));
        SerializationObjectTypedSharedData::enumerateTypedSharedDataStreams(
            settings, callback, SerializationObjectTypedSharedData::pathsSizeHint(data.deserialize_state, first_path_index + i, 1));
        settings.path.pop_back();
    }
}

void SerializationSubObjectTypedSharedData::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationSubObjectTypedSharedData");
}

void SerializationSubObjectTypedSharedData::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationSubObjectTypedSharedData");
}

void SerializationSubObjectTypedSharedData::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationSubObjectTypedSharedData");
}

void SerializationSubObjectTypedSharedData::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    if (settings.data_part_type == MergeTreeDataPartType::Unknown)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sub-object {} of the typed shared data of Object is read outside a data part", paths_prefix);

    /// The caller has pushed ObjectData and ObjectTypedSharedData. The state is the structure state of the column,
    /// shared with its other readers, with the sub-object's paths requested on it.
    auto structure_state = SerializationObjectTypedSharedData::deserializeStructureStatePrefix(settings, cache, num_paths);
    if (!structure_state)
        return;

    auto * structure
        = checkAndGetState<SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedDataStructure>(structure_state);
    for (size_t i = 0; i != paths.size(); ++i)
        structure->requestPath(first_path_index + i, paths[i]);
    state = std::move(structure_state);
}

void SerializationSubObjectTypedSharedData::deserializeBinaryBulkWithMultipleStreams(
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
    auto & typed_paths = assert_cast<ColumnObject &>(column).getTypedPaths();
    auto own_column = [&](size_t i) -> IColumn & { return *typed_paths.at(paths[i].name.substr(paths_prefix.size())); };

    /// The caller has pushed ObjectData and ObjectTypedSharedData. The first reader of a call decodes every requested
    /// path, so either every path is in the cache or none is.
    if (SerializationObjectTypedSharedData::insertPathFromCache(own_column(0), paths.front().name, settings, cache))
    {
        for (size_t i = 1; i != paths.size(); ++i)
        {
            if (!SerializationObjectTypedSharedData::insertPathFromCache(own_column(i), paths[i].name, settings, cache))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object is not in the substreams cache", paths[i].name);
        }
        return;
    }

    SerializationObjectTypedSharedData::deserializeRequestedPaths(
        limit,
        settings,
        *structure,
        cache,
        [&](size_t path_index) -> IColumn *
        {
            if (path_index < first_path_index || path_index >= first_path_index + paths.size())
                return nullptr;
            return &own_column(path_index - first_path_index);
        });
}

}
