#include <GPU/GPUHashTable.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUTypeMapping.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event GPUJoinBuildRows;
    extern const Event GPUJoinProbeRows;
    extern const Event GPUJoinMatchedRows;
    extern const Event GPUJoinMicroseconds;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

namespace
{

size_t rowBytesOf(GPUElementType key_element_type, const std::vector<GPUElementType> & payload_element_types)
{
    size_t bytes = sizeOf(key_element_type);
    for (const GPUElementType payload_element_type : payload_element_types)
        bytes += sizeOf(payload_element_type);
    return bytes;
}

}

bool HashTable::canJoinOnDevice(const IDataType & key_type, const DataTypes & payload_types_to_check)
{
    const std::optional<GPUElementType> key_element_type = elementTypeOf(key_type);
    if (!key_element_type || !isInteger(*key_element_type))
        return false;

    for (const auto & type : payload_types_to_check)
    {
        if (!elementTypeOf(*type))
            return false;
    }

    return true;
}

HashTable::HashTable(const IDataType & key_type, const DataTypes & payload_types_, size_t stage_bytes)
    : key_element_type(elementTypeOrThrow(key_type))
    , payload_types(payload_types_)
    , payload_element_types(elementTypesOrThrow(payload_types))
    , row_bytes(rowBytesOf(key_element_type, payload_element_types))
    , build_key_pipe(key_type, stage_bytes)
    , probe_key_pipe(key_type, stage_bytes)
    , hash_join(onDevice(
          [&] { return IHashJoin::create(key_element_type, payload_element_types); },
          "Cannot set up a hash join on {} with {} payload columns on the device",
          key_type.getName(),
          payload_types.size()))
{
    build_payload_pipes.reserve(payload_types.size());
    for (const auto & type : payload_types)
        build_payload_pipes.emplace_back(*type, stage_bytes);
}

void HashTable::addBuildBlock(const IColumn & key_column, const Columns & payload_columns)
{
    if (built)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of the right table arrived after the GPU hash table was built");

    const size_t num_rows = key_column.size();
    if (num_rows == 0)
        return;

    if (payload_columns.size() != build_payload_pipes.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The right table block carries {} payload columns, expected {}",
            payload_columns.size(),
            build_payload_pipes.size());

    Stopwatch watch;

    build_key_pipe.stage(key_column);
    for (size_t i = 0; i < payload_columns.size(); ++i)
        build_payload_pipes[i].stage(*payload_columns[i]);

    build_rows += num_rows;
    build_bytes += num_rows * row_bytes;

    ProfileEvents::increment(ProfileEvents::GPUJoinBuildRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUJoinMicroseconds, watch.elapsedMicroseconds());
}

void HashTable::finishBuild()
{
    if (built)
        return;

    built = true;

    if (build_rows == 0)
        return;

    Stopwatch watch;

    std::vector<DeviceColumnView> payloads;
    payloads.reserve(build_payload_pipes.size());
    for (auto & pipe : build_payload_pipes)
        payloads.push_back(pipe.flush().view());

    const DeviceColumnView keys = build_key_pipe.flush().view();
    if (keys.rows != build_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "The device holds {} keys of the right table, expected {}", keys.rows, build_rows);

    onDevice(
        [&] { hash_join->build(keys, payloads); }, "Cannot build a hash table over {} rows of the right table on a GPU", build_rows);

    ProfileEvents::increment(ProfileEvents::GPUJoinMicroseconds, watch.elapsedMicroseconds());
}

HashTable::Matches HashTable::noMatches() const
{
    Matches matches{ColumnUInt32::create(), MutableColumns{}};

    matches.build_payload_columns.reserve(payload_types.size());
    for (const auto & type : payload_types)
        matches.build_payload_columns.push_back(type->createColumn());

    return matches;
}

HashTable::Matches HashTable::probe(const IColumn & key_column)
{
    if (!built)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The device hash table is probed before it is built");

    Matches matches = noMatches();

    const size_t num_rows = key_column.size();
    if (num_rows == 0 || build_rows == 0)
        return matches;

    Stopwatch watch;

    probe_key_pipe.reset();
    probe_key_pipe.stage(key_column);
    const DeviceColumnView keys = probe_key_pipe.flush().view();

    const size_t num_matches = onDevice(
        [&] { return hash_join->probe(keys); }, "Cannot probe the GPU's hash table with {} rows of the left table", num_rows);

    if (num_matches != 0)
    {
        std::vector<HostColumnView> payloads;
        payloads.reserve(matches.build_payload_columns.size());
        for (size_t i = 0; i < matches.build_payload_columns.size(); ++i)
            payloads.push_back(resizeForElementType(*matches.build_payload_columns[i], num_matches, payload_element_types[i]));

        const HostColumnView probe_row_indices
            = resizeForElementType(*matches.probe_row_indices, num_matches, GPUElementType::UInt32);

        onDevice(
            [&] { hash_join->copyMatchesOut(probe_row_indices, payloads); }, "Cannot copy {} joined rows back from the device", num_matches);
    }

    ProfileEvents::increment(ProfileEvents::GPUJoinMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::GPUJoinProbeRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUJoinMatchedRows, num_matches);

    return matches;
}

}

#endif
