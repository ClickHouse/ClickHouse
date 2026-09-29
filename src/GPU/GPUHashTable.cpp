#include <GPU/GPUHashTable.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUTypeMapping.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <string_view>

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

void checkMatches(const DeviceFixedColumn & column, size_t num_matches, std::string_view what)
{
    if (column.rows != num_matches)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The device returned {} {} for {} matches", column.rows, what, num_matches);
}

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

HashTable::HashTable(const IDataType & key_type_, const DataTypes & payload_types_, size_t stage_bytes_)
    : key_type(key_type_.getPtr())
    , key_element_type(elementTypeOrThrow(*key_type))
    , payload_types(payload_types_)
    , payload_element_types(elementTypesOrThrow(payload_types))
    , row_bytes(rowBytesOf(key_element_type, payload_element_types))
    , stage_bytes(stage_bytes_)
    , build_key_pipe(*key_type, stage_bytes)
    , hash_join(onDevice(
          [&] { return std::make_unique<CudfHashJoin>(key_element_type, payload_element_types); },
          "Cannot set up a hash join on {} with {} payload columns on the device",
          key_type->getName(),
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

    std::vector<DeviceFixedColumn> payloads;
    payloads.reserve(build_payload_pipes.size());
    for (auto & pipe : build_payload_pipes)
        payloads.push_back(fixedOrThrow(pipe.flush().view()));

    const DeviceFixedColumn keys = fixedOrThrow(build_key_pipe.flush().view());
    if (keys.rows != build_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "The device holds {} keys of the right table, expected {}", keys.rows, build_rows);

    onDevice(
        [&] { hash_join->build(keys, payloads); }, "Cannot build a hash table over {} rows of the right table on a GPU", build_rows);

    synchronizeDevice();

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
        [&] { return std::make_unique<Probe>(*hash_join, *key_type, stage_bytes); },
        "Cannot set up a probe of the GPU's hash table");
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

    std::unique_ptr<Probe> probe = takeProbe();

    probe->keys.stage(key_column);
    const DeviceFixedColumn keys = fixedOrThrow(probe->keys.flush().view());

    const size_t num_matches = onDevice(
        [&] { return probe->device.probe(keys); }, "Cannot probe the GPU's hash table with {} rows of the left table", num_rows);

    if (num_matches != 0)
    {
        std::vector<DeviceColumnView> from;
        std::vector<IColumn *> to;
        from.reserve(payload_element_types.size() + 1);
        to.reserve(payload_element_types.size() + 1);

        const DeviceFixedColumn device_indices
            = onDevice([&] { return probe->device.probeRowIndices(); }, "Cannot view the probe-side row indices on the device");
        checkMatches(device_indices, num_matches, "probe-side row indices");
        from.push_back(device_indices);
        to.push_back(matches.probe_row_indices.get());

        for (size_t i = 0; i < payload_element_types.size(); ++i)
        {
            const DeviceFixedColumn device_payload
                = onDevice([&] { return probe->device.gatheredPayload(i); }, "Cannot view gathered payload column {} on the device", i);
            checkMatches(device_payload, num_matches, "gathered payload rows");
            from.push_back(device_payload);
            to.push_back(matches.build_payload_columns[i].get());
        }

        copyDeviceToHost(from, to, probe->stream.get());
    }

    probe->keys.reset();
    returnProbe(std::move(probe));

    ProfileEvents::increment(ProfileEvents::GPUJoinMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::GPUJoinProbeRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUJoinMatchedRows, num_matches);

    return matches;
}

}

#endif
