#include <Columns/ColumnObject.h>
#include <DataTypes/Serializations/SerializationObjectTypedSharedDataPath.h>
#include <DataTypes/Serializations/SerializationSubObjectTypedSharedData.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

struct DeserializeBinaryBulkStateSubObjectTypedSharedData : public ISerialization::DeserializeBinaryBulkState
{
    /// One per path reader.
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> path_states;

    ISerialization::DeserializeBinaryBulkStatePtr cloneImpl(ISerialization::DeserializeBinaryBulkStateCloneContext & context) const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateSubObjectTypedSharedData>();
        for (const auto & path_state : path_states)
            new_state->path_states.push_back(path_state ? path_state->clone(context) : nullptr);
        return new_state;
    }
};

/// The path readers push ObjectData and ObjectTypedSharedData themselves, which the sub-object has pushed already, so
/// the two come off the path around their calls.
template <typename F>
void callPathReaders(ISerialization::SubstreamPath & path, F && call)
{
    using Substream = ISerialization::Substream;
    if (path.size() < 2 || path.back().type != Substream::ObjectTypedSharedData || path[path.size() - 2].type != Substream::ObjectData)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected substream path of a sub-object of the typed shared data of Object");

    auto typed_shared_data = std::move(path.back());
    path.pop_back();
    auto object_data = std::move(path.back());
    path.pop_back();
    SCOPE_EXIT({
        path.push_back(std::move(object_data));
        path.push_back(std::move(typed_shared_data));
    });
    call();
}

}

SerializationSubObjectTypedSharedData::SerializationSubObjectTypedSharedData(
    const String & paths_prefix_, SharedTypedPaths paths_, size_t first_path_index_, size_t num_paths_)
    : paths_prefix(paths_prefix_)
    , paths(std::move(paths_))
    , first_path_index(first_path_index_)
    , num_paths(num_paths_)
{
    path_readers.reserve(paths.size());
    for (size_t i = 0; i != paths.size(); ++i)
    {
        const auto & path = paths[i];
        path_readers.push_back(SerializationObjectTypedSharedDataPath::create(path.serialization, path, first_path_index + i, num_paths, ""));
    }
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
    /// The path readers are pooled and accounted by the pool.
    size_t bytes = sizeof(*this) + paths_prefix.capacity() + paths.capacity() * sizeof(SharedTypedPaths::value_type)
        + path_readers.capacity() * sizeof(SerializationPtr);
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
    const auto * state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateSubObjectTypedSharedData>(data.deserialize_state) : nullptr;
    callPathReaders(
        settings.path,
        [&]
        {
            for (size_t i = 0; i != path_readers.size(); ++i)
            {
                auto path_data = SubstreamData(path_readers[i]);
                if (state)
                    path_data.withDeserializeState(state->path_states[i]);
                path_readers[i]->enumerateStreams(settings, callback, path_data);
            }
        });
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
    auto sub_object_state = std::make_shared<DeserializeBinaryBulkStateSubObjectTypedSharedData>();
    sub_object_state->path_states.resize(path_readers.size());
    callPathReaders(
        settings.path,
        [&]
        {
            for (size_t i = 0; i != path_readers.size(); ++i)
                path_readers[i]->deserializeBinaryBulkStatePrefix(settings, sub_object_state->path_states[i], cache);
        });
    state = std::move(sub_object_state);
}

void SerializationSubObjectTypedSharedData::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * sub_object_state = checkAndGetState<DeserializeBinaryBulkStateSubObjectTypedSharedData>(state);
    auto & typed_paths = assert_cast<ColumnObject &>(column).getTypedPaths();
    callPathReaders(
        settings.path,
        [&]
        {
            for (size_t i = 0; i != path_readers.size(); ++i)
                path_readers[i]->deserializeBinaryBulkWithMultipleStreams(
                    *typed_paths.at(paths[i].name.substr(paths_prefix.size())), limit, settings, sub_object_state->path_states[i], cache);
        });
}

}
