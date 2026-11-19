#include <Columns/ColumnObject.h>
#include <DataTypes/Serializations/SerializationSubObjectTypedSharedData.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <ranges>

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
    const auto * reader_state = data.deserialize_state
        ? checkAndGetState<SerializationObjectTypedSharedData::DeserializeBinaryBulkStatePathsReader>(data.deserialize_state)
        : nullptr;

    /// The caller has pushed ObjectData and ObjectTypedSharedData. Each path is listed behind its identity element:
    /// read over its regions, the header and the streams of the path's serialization with the views of their regions;
    /// otherwise its bytes of the plain layout's data stream. Only the prefixes tell the layout: without them only the
    /// header, which they read, is sure.
    if (!reader_state)
    {
        settings.path.push_back(SerializationObjectTypedSharedData::getPathElement(paths.front().name));
        settings.path.push_back(Substream::ObjectTypedSharedDataHeader);
        callback(settings.path);
        settings.path.pop_back();
        settings.path.pop_back();
        return;
    }

    auto * shared_state = checkAndGetState<SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedData>(
        reader_state->shared_state);
    for (size_t i = 0; i != paths.size(); ++i)
    {
        settings.path.push_back(SerializationObjectTypedSharedData::getPathElement(paths[i].name));
        if (shared_state->readsFromRegions(first_path_index + i))
        {
            settings.path.push_back(Substream::ObjectTypedSharedDataHeader);
            callback(settings.path);
            settings.path.pop_back();

            auto path_data = SubstreamData(paths[i].serialization).withType(paths[i].type).withDeserializeState(reader_state->path_states[i]);
            SerializationObjectTypedSharedData::enumeratePathRegionsStreams(settings, callback, *shared_state, first_path_index + i, path_data);
        }
        else
        {
            SerializationObjectTypedSharedData::enumerateTypedSharedDataStreams(
                settings, callback, SerializationObjectTypedSharedData::pathsSizeHint(reader_state->shared_state, first_path_index + i, 1));
        }
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

    /// The caller has pushed ObjectData and ObjectTypedSharedData. The state is the column's shared state, with the
    /// sub-object's paths requested on it.
    auto state_ptr = SerializationObjectTypedSharedData::deserializeSharedState(settings, cache, num_paths);
    if (!state_ptr)
        return;

    auto * shared_state
        = checkAndGetState<SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedData>(state_ptr);
    /// A sub-object wider than the threshold reads its paths from the plain layout rather than over their regions.
    bool from_plain_layout = paths.size() > settings.object_shared_data_max_sub_object_paths_to_read_separately;
    auto reader_state = std::make_shared<SerializationObjectTypedSharedData::DeserializeBinaryBulkStatePathsReader>();
    reader_state->path_states.resize(paths.size());
    for (size_t i = 0; i != paths.size(); ++i)
    {
        shared_state->requestPath(first_path_index + i, paths[i], from_plain_layout);
        if (!shared_state->readsFromRegions(first_path_index + i))
            continue;
        settings.path.push_back(SerializationObjectTypedSharedData::getPathElement(paths[i].name));
        SCOPE_EXIT(settings.path.pop_back());
        SerializationObjectTypedSharedData::deserializePathRegionsPrefix(
            settings, *shared_state, first_path_index + i, *paths[i].serialization, reader_state->path_states[i], cache);
    }
    reader_state->shared_state = std::move(state_ptr);
    state = std::move(reader_state);
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

    auto * reader_state = checkAndGetState<SerializationObjectTypedSharedData::DeserializeBinaryBulkStatePathsReader>(state);
    auto * shared_state = checkAndGetState<SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedData>(
        reader_state->shared_state);
    auto & typed_paths = assert_cast<ColumnObject &>(column).getTypedPaths();
    auto own_column = [&](size_t i) -> IColumn & { return *typed_paths.at(paths[i].name.substr(paths_prefix.size())); };

    /// The caller has pushed ObjectData and ObjectTypedSharedData. A path read over its regions is read on its own, and
    /// shares its streams with the other readers of the path through the substreams cache, as a Wide column does.
    /// Another reader may have flagged some of the paths for the plain layout, so the sub-object can read from both.
    std::vector<size_t> plain_layout_paths;
    for (size_t i = 0; i != paths.size(); ++i)
    {
        if (!shared_state->readsFromRegions(first_path_index + i))
        {
            plain_layout_paths.push_back(i);
            continue;
        }
        settings.path.push_back(SerializationObjectTypedSharedData::getPathElement(paths[i].name));
        SCOPE_EXIT(settings.path.pop_back());
        SerializationObjectTypedSharedData::deserializePathRegions(
            own_column(i), limit, settings, *shared_state, first_path_index + i, *paths[i].serialization, reader_state->path_states[i], cache);
    }

    if (plain_layout_paths.empty())
        return;

    /// The first reader of the plain layout in a call decodes every path read from it, so either all of them are in the
    /// cache or none is.
    if (SerializationObjectTypedSharedData::insertPathFromCache(
            own_column(plain_layout_paths.front()), paths[plain_layout_paths.front()].name, settings, cache))
    {
        for (size_t i : plain_layout_paths | std::views::drop(1))
        {
            if (!SerializationObjectTypedSharedData::insertPathFromCache(own_column(i), paths[i].name, settings, cache))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object is not in the substreams cache", paths[i].name);
        }
        return;
    }

    /// The decode asks only for the paths it reads from the plain layout.
    SerializationObjectTypedSharedData::deserializePlainLayoutPaths(
        limit,
        settings,
        *shared_state,
        cache,
        [&](size_t path_index) -> IColumn *
        {
            if (path_index < first_path_index || path_index >= first_path_index + paths.size())
                return nullptr;
            return &own_column(path_index - first_path_index);
        });
}

}
