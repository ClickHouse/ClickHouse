#include <GPU/GPUHashTable.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUTypeMapping.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <cstring>

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
    , hash_join(onDevice(
          [&] { return std::make_unique<CudfHashJoin>(key_element_type, payload_element_types); },
          "Cannot set up a hash join on {} with {} payload columns on the device",
          key_type.getName(),
          payload_types.size()))
{
    build_payload_pipes.reserve(payload_types.size());
    for (const auto & type : payload_types)
        build_payload_pipes.emplace_back(*type, stage_bytes);
}

HashTable::~HashTable() = default;

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
    {
        ready.store(true, std::memory_order_release);
        return;
    }

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

    ready.store(true, std::memory_order_release);

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

std::unique_ptr<HashTable::Probe> HashTable::takeProbe()
{
    {
        std::lock_guard lock(idle_probes_mutex);
        if (!idle_probes.empty())
        {
            std::unique_ptr<Probe> probe = std::move(idle_probes.back());
            idle_probes.pop_back();
            return probe;
        }
    }

    return onDevice(
        [&] { return std::make_unique<Probe>(*hash_join, payload_types.size()); }, "Cannot set up a probe of the GPU's hash table");
}

void HashTable::returnProbe(std::unique_ptr<Probe> probe)
{
    std::lock_guard lock(idle_probes_mutex);
    idle_probes.push_back(std::move(probe));
}

HashTable::Matches HashTable::probe(const IColumn & key_column)
{
    if (!ready.load(std::memory_order_acquire))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The device hash table is probed before it is built");

    Matches matches = noMatches();

    const size_t num_rows = key_column.size();
    if (num_rows == 0 || build_rows == 0)
        return matches;

    Stopwatch watch;

    /// A probe that failed may still have work queued on its stream, and is dropped rather than
    /// returned: its destructor waits for the stream.
    std::unique_ptr<Probe> probe = takeProbe();

    probe->keys.clear();
    probe->keys.append(rawValuesOf(key_column, num_rows, sizeOf(key_element_type)));

    const size_t num_matches = onDevice(
        [&] { return probe->device.probe(probe->keys.data(), num_rows); },
        "Cannot probe the GPU's hash table with {} rows of the left table",
        num_rows);

    if (num_matches != 0)
    {
        probe->probe_row_indices.clear();
        const HostColumnView probe_row_indices{
            GPUElementType::UInt32, probe->probe_row_indices.grow(num_matches * sizeof(UInt32)), num_matches};

        std::vector<HostColumnView> payloads;
        payloads.reserve(payload_element_types.size());
        for (size_t i = 0; i < payload_element_types.size(); ++i)
        {
            probe->payloads[i].clear();
            payloads.push_back(
                {payload_element_types[i], probe->payloads[i].grow(num_matches * sizeOf(payload_element_types[i])), num_matches});
        }

        onDevice(
            [&] { probe->device.copyMatchesOut(probe_row_indices, payloads); }, "Cannot copy {} joined rows back from the device", num_matches);

        const HostColumnView indices_column = resizeForElementType(*matches.probe_row_indices, num_matches, GPUElementType::UInt32);
        memcpy(indices_column.data, probe_row_indices.data, num_matches * sizeof(UInt32));

        for (size_t i = 0; i < payloads.size(); ++i)
        {
            const HostColumnView payload_column
                = resizeForElementType(*matches.build_payload_columns[i], num_matches, payload_element_types[i]);
            memcpy(payload_column.data, payloads[i].data, num_matches * sizeOf(payload_element_types[i]));
        }
    }

    returnProbe(std::move(probe));

    ProfileEvents::increment(ProfileEvents::GPUJoinMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::GPUJoinProbeRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUJoinMatchedRows, num_matches);

    return matches;
}

}

#endif
