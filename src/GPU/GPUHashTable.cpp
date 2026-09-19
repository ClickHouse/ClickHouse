#include <GPU/GPUHashTable.h>

#if USE_GPU

#include <GPU/GPUAccumulator.h>
#include <GPU/GPUCall.h>
#include <GPU/GPUTypes.h>

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
    extern const int GPU_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

namespace
{

constexpr size_t error_buffer_size = 1024;


GPUElementType elementTypeOrThrow(const IDataType & type)
{
    const std::optional<GPUElementType> element_type = elementTypeOf(type);
    if (!element_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Type {} cannot be sent to the device", type.getName());
    return *element_type;
}

std::vector<GPUElementType> elementTypesOf(const DataTypes & types)
{
    std::vector<GPUElementType> element_types;
    element_types.reserve(types.size());

    for (const auto & type : types)
        element_types.push_back(elementTypeOrThrow(*type));

    return element_types;
}

std::vector<size_t> elementSizesOf(const std::vector<GPUElementType> & element_types)
{
    std::vector<size_t> sizes;
    sizes.reserve(element_types.size());

    for (const GPUElementType element_type : element_types)
        sizes.push_back(sizeOf(element_type));

    return sizes;
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

void GPUHashTableDeleter::operator()(GPUHashTableState * hash_table) const noexcept
{
    destroyGPUHashTable(hash_table);
}

HashTable::HashTable(const IDataType & key_type, const DataTypes & payload_types_, size_t stage_bytes)
    : key_element_type(elementTypeOrThrow(key_type))
    , key_element_size(sizeOf(key_element_type))
    , payload_types(payload_types_)
    , payload_element_types(elementTypesOf(payload_types))
    , payload_element_sizes(elementSizesOf(payload_element_types))
    , build_key_pipe(key_type, stage_bytes)
    , probe_key_pipe(key_type, stage_bytes)
{
    build_payload_pipes.reserve(payload_types.size());
    for (const auto & type : payload_types)
        build_payload_pipes.emplace_back(*type, stage_bytes);

    GPUHashTableState * created = nullptr;

    call([&](char * e, size_t n)
         { return createGPUHashTable(
               key_element_type, payload_element_types.data(), payload_element_types.size(), &created, e, n); },
         "Cannot set up a hash join on the device");

    hash_table_on_gpu.reset(created);
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

    size_t row_bytes = key_element_size;
    for (const size_t element_size : payload_element_sizes)
        row_bytes += element_size;

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

    std::vector<const GPUBuffer *> payload_buffers(build_payload_pipes.size());
    for (size_t i = 0; i < build_payload_pipes.size(); ++i)
        payload_buffers[i] = build_payload_pipes[i].flush().buffer();

    const GPUBuffer * key_buffer = build_key_pipe.flush().buffer();

    call([&](char * e, size_t n)
         { return setGPUHashTableBuildSide(
               hash_table_on_gpu.get(),
               key_buffer,
               payload_buffers.data(),
               payload_buffers.size(),
               build_rows,
               e,
               n); },
         "Cannot hand the GPU's own copy of the right table to the join");

    call([&](char * e, size_t n)
         { return buildGPUHashTable(hash_table_on_gpu.get(), e, n); },
         "Cannot build a hash table over {} rows of the right table on a GPU",
         build_rows);

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

    char error[error_buffer_size] = {};
    size_t num_matches = 0;

    Stopwatch watch;

    probe_key_pipe.reset();
    probe_key_pipe.stage(key_column);
    const GPUBuffer * key_buffer = probe_key_pipe.flush().buffer();

    int status = probeGPUHashTable(
        hash_table_on_gpu.get(), key_buffer, num_rows, &num_matches, error, sizeof(error));

    if (status != 0)
        throw Exception(
            ErrorCodes::GPU_ERROR, "Cannot probe the GPU's hash table with {} rows of the left table: {}", num_rows, error);

    if (num_matches != 0)
    {
        std::vector<void *> payload_destinations(matches.build_payload_columns.size());
        for (size_t i = 0; i < payload_destinations.size(); ++i)
            payload_destinations[i]
                = resizeForElementType(*matches.build_payload_columns[i], num_matches, payload_element_types[i]);

        matches.probe_row_indices->getData().resize(num_matches);

        status = copyGPUMatchesOut(
            hash_table_on_gpu.get(),
            matches.probe_row_indices->getData().data(),
            payload_destinations.data(),
            error,
            sizeof(error));

        if (status != 0)
            throw Exception(ErrorCodes::GPU_ERROR, "Cannot copy {} joined rows back from the device: {}", num_matches, error);
    }

    ProfileEvents::increment(ProfileEvents::GPUJoinMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::GPUJoinProbeRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUJoinMatchedRows, num_matches);

    return matches;
}

}

#endif
