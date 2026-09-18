#include <GPU/GPUAggregation.h>

#if USE_GPU

#include <GPU/GPUAggregationCudf.h>

#include <Columns/ColumnVector.h>
#include <Compression/CompressionInfo.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <map>
#include <mutex>
#include <numeric>
#include <optional>
#include <utility>

namespace ProfileEvents
{
    extern const Event GPUAggregationRows;
    extern const Event GPUAggregationBatches;
    extern const Event GPUAggregationMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int GPU_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace GPU
{
namespace
{
constexpr size_t error_buffer_size = 1024;
}

namespace
{

class PinnedBufferPool
{
public:
    static PinnedBufferPool & instance()
    {
        static PinnedBufferPool pool;
        return pool;
    }

    std::pair<char *, size_t> acquire(size_t bytes)
    {
        {
            std::lock_guard lock(mutex);
            const auto it = free_buffers.lower_bound(bytes);
            if (it != free_buffers.end())
            {
                const std::pair<char *, size_t> taken{it->second, it->first};
                pooled_bytes -= it->first;
                free_buffers.erase(it);
                return taken;
            }
        }

        void * fresh = nullptr;
        char error[error_buffer_size] = {};
        if (clickhouseGPUAllocPinned(bytes, &fresh, error, sizeof(error)) != 0)
            throw Exception(
                ErrorCodes::GPU_ERROR, "Cannot allocate {} bytes of pinned host memory: {}", bytes, error);

        return {static_cast<char *>(fresh), bytes};
    }

    void release(char * buffer, size_t capacity) noexcept
    {
        if (buffer == nullptr)
            return;

        {
            std::lock_guard lock(mutex);
            if (pooled_bytes + capacity <= max_pooled_bytes)
            {
                free_buffers.emplace(capacity, buffer);
                pooled_bytes += capacity;
                return;
            }
        }

        clickhouseGPUFreePinned(buffer);
    }

private:
    ~PinnedBufferPool() = default;

    static constexpr size_t max_pooled_bytes = 1024UL * 1024 * 1024;

    std::mutex mutex;
    std::multimap<size_t, char *> free_buffers TSA_GUARDED_BY(mutex);
    size_t pooled_bytes TSA_GUARDED_BY(mutex) = 0;
};

}

PinnedBuffer::~PinnedBuffer()
{
    PinnedBufferPool::instance().release(buffer, capacity);
}

PinnedBuffer::PinnedBuffer(PinnedBuffer && other) noexcept
    : buffer(other.buffer), capacity(other.capacity), used(other.used)
{
    other.buffer = nullptr;
    other.capacity = 0;
    other.used = 0;
}

PinnedBuffer & PinnedBuffer::operator=(PinnedBuffer && other) noexcept
{
    if (this != &other)
    {
        clickhouseGPUFreePinned(buffer);
        buffer = other.buffer;
        capacity = other.capacity;
        used = other.used;
        other.buffer = nullptr;
        other.capacity = 0;
        other.used = 0;
    }
    return *this;
}

void PinnedBuffer::reserve(size_t bytes)
{
    if (bytes <= capacity)
        return;

    const size_t new_capacity = std::max(bytes, capacity * 2);

    const auto [fresh, fresh_capacity] = PinnedBufferPool::instance().acquire(new_capacity);

    if (used != 0)
        memcpy(fresh, buffer, used);

    PinnedBufferPool::instance().release(buffer, capacity);
    buffer = fresh;
    capacity = fresh_capacity;
}

void PinnedBuffer::append(const char * data, size_t bytes)
{
    if (bytes == 0)
        return;

    reserve(used + bytes);
    memcpy(buffer + used, data, bytes);
    used += bytes;
}

std::optional<int> elementTypeOf(const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::UInt8: return CLICKHOUSE_GPU_ELEMENT_UINT8;
        case TypeIndex::UInt16: return CLICKHOUSE_GPU_ELEMENT_UINT16;
        case TypeIndex::UInt32: return CLICKHOUSE_GPU_ELEMENT_UINT32;
        case TypeIndex::UInt64: return CLICKHOUSE_GPU_ELEMENT_UINT64;
        case TypeIndex::Int8: return CLICKHOUSE_GPU_ELEMENT_INT8;
        case TypeIndex::Int16: return CLICKHOUSE_GPU_ELEMENT_INT16;
        case TypeIndex::Int32: return CLICKHOUSE_GPU_ELEMENT_INT32;
        case TypeIndex::Int64: return CLICKHOUSE_GPU_ELEMENT_INT64;
        case TypeIndex::Float32: return CLICKHOUSE_GPU_ELEMENT_FLOAT32;
        case TypeIndex::Float64: return CLICKHOUSE_GPU_ELEMENT_FLOAT64;
        default: return {};
    }
}

size_t elementSizeOf(int element_type)
{
    switch (element_type)
    {
        case CLICKHOUSE_GPU_ELEMENT_UINT8:
        case CLICKHOUSE_GPU_ELEMENT_INT8:
            return 1;
        case CLICKHOUSE_GPU_ELEMENT_UINT16:
        case CLICKHOUSE_GPU_ELEMENT_INT16:
            return 2;
        case CLICKHOUSE_GPU_ELEMENT_UINT32:
        case CLICKHOUSE_GPU_ELEMENT_INT32:
        case CLICKHOUSE_GPU_ELEMENT_FLOAT32:
            return 4;
        case CLICKHOUSE_GPU_ELEMENT_UINT64:
        case CLICKHOUSE_GPU_ELEMENT_INT64:
        case CLICKHOUSE_GPU_ELEMENT_FLOAT64:
            return 8;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU element type {}", element_type);
    }
}

namespace
{

std::optional<int> resultTypeFor(int element_type)
{
    switch (element_type)
    {
        case CLICKHOUSE_GPU_ELEMENT_UINT8:
        case CLICKHOUSE_GPU_ELEMENT_UINT16:
        case CLICKHOUSE_GPU_ELEMENT_UINT32:
        case CLICKHOUSE_GPU_ELEMENT_UINT64:
            return CLICKHOUSE_GPU_RESULT_UINT64;
        case CLICKHOUSE_GPU_ELEMENT_INT8:
        case CLICKHOUSE_GPU_ELEMENT_INT16:
        case CLICKHOUSE_GPU_ELEMENT_INT32:
        case CLICKHOUSE_GPU_ELEMENT_INT64:
            return CLICKHOUSE_GPU_RESULT_INT64;
        case CLICKHOUSE_GPU_ELEMENT_FLOAT32:
        case CLICKHOUSE_GPU_ELEMENT_FLOAT64:
            return CLICKHOUSE_GPU_RESULT_FLOAT64;
        default:
            return {};
    }
}

std::optional<int> resultTypeOf(const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::UInt64: return CLICKHOUSE_GPU_RESULT_UINT64;
        case TypeIndex::Int64: return CLICKHOUSE_GPU_RESULT_INT64;
        case TypeIndex::Float64: return CLICKHOUSE_GPU_RESULT_FLOAT64;
        default: return {};
    }
}

String aggregationName(int aggregation)
{
    switch (aggregation)
    {
        case CLICKHOUSE_GPU_AGGREGATION_SUM: return "sum";
        case CLICKHOUSE_GPU_AGGREGATION_MIN: return "min";
        case CLICKHOUSE_GPU_AGGREGATION_MAX: return "max";
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU aggregation {}", aggregation);
    }
}
}

std::optional<int> aggregationOf(const String & aggregate_function_name)
{
    if (aggregate_function_name == "sum")
        return CLICKHOUSE_GPU_AGGREGATION_SUM;
    if (aggregate_function_name == "min")
        return CLICKHOUSE_GPU_AGGREGATION_MIN;
    if (aggregate_function_name == "max")
        return CLICKHOUSE_GPU_AGGREGATION_MAX;

    return {};
}

std::optional<int> codecOf(UInt8 method_byte)
{
    switch (method_byte)
    {
        case static_cast<UInt8>(CompressionMethodByte::LZ4): return CLICKHOUSE_GPU_CODEC_LZ4;
        case static_cast<UInt8>(CompressionMethodByte::ZSTD): return CLICKHOUSE_GPU_CODEC_ZSTD;
        default: return {};
    }
}

const String & deviceProbeError()
{
    static const String error = []
    {
        char message[error_buffer_size] = {};
        if (clickhouseGPUProbeDevice(message, sizeof(message)) == 0)
            return String{};
        return String{message};
    }();

    return error;
}

bool canReduceOnDevice(const IDataType & argument_type, const IDataType & result_type, int aggregation)
{
    const auto element_type = elementTypeOf(argument_type);
    if (!element_type)
        return false;

    if (aggregation == CLICKHOUSE_GPU_AGGREGATION_SUM)
    {
        const auto result = resultTypeOf(result_type);
        return result && result == resultTypeFor(*element_type);
    }

    return elementTypeOf(result_type) == element_type;
}

namespace
{
int aggregationOrThrow(int aggregation)
{
    aggregationName(aggregation);
    return aggregation;
}

int elementTypeOrThrow(const IDataType & argument_type, const IDataType & result_type, int aggregation)
{
    if (canReduceOnDevice(argument_type, result_type, aggregation))
        return *elementTypeOf(argument_type);

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot reduce a column of {} into {} by `{}` on a GPU",
        argument_type.getName(),
        result_type.getName(),
        aggregationName(aggregation));
}

int resultTypeOrThrow(int element_type)
{
    if (const auto result_type = resultTypeFor(element_type))
        return *result_type;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU element type {}", element_type);
}
}

GPUAccumulator::GPUAccumulator(
    const IDataType & argument_type,
    const IDataType & result_type_,
    int aggregation_,
    size_t batch_bytes_,
    std::optional<int> codec_)
    : element_type(elementTypeOrThrow(argument_type, result_type_, aggregationOrThrow(aggregation_)))
    , result_type(resultTypeOrThrow(element_type))
    , aggregation(aggregation_)
    , element_size(elementSizeOf(element_type))
    , batch_bytes(std::clamp(batch_bytes_, element_size, max_batch_rows * element_size))
    , codec(codec_)
{
    if (!codec)
        staged.reserve(batch_bytes);
}

void GPUAccumulator::flushIfBatchWouldOverflow(size_t incoming_rows, size_t incoming_bytes)
{
    if (staged_values_bytes == 0)
        return;

    if (staged_values_bytes + incoming_bytes > batch_bytes
        || staged_values_bytes / element_size + incoming_rows > max_batch_rows)
        reduceBatchOnDevice();
}

void GPUAccumulator::add(const IColumn & column)
{
    if (!block_offsets.empty())
        throw Exception(ErrorCodes::GPU_ERROR, "A batch of compressed blocks cannot also take plain values");

    const std::string_view raw = column.getRawData();
    if (raw.size() != column.size() * element_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column {} of {} rows holds {} bytes of values, expected {}",
            column.getName(),
            column.size(),
            raw.size(),
            column.size() * element_size);

    flushIfBatchWouldOverflow(column.size(), raw.size());

    staged.append(raw.data(), raw.size());
    staged_values_bytes += raw.size();

    if (staged_values_bytes >= batch_bytes)
        reduceBatchOnDevice();
}

void GPUAccumulator::addBlock(const char * payload, size_t compressed_bytes, size_t decompressed_bytes)
{
    if (!codec)
        throw Exception(ErrorCodes::GPU_ERROR, "A compressed block needs a codec the device can expand");

    if (staged_values_bytes != 0 && block_offsets.empty())
        throw Exception(ErrorCodes::GPU_ERROR, "A batch of plain values cannot also take compressed blocks");

    flushIfBatchWouldOverflow(decompressed_bytes / element_size, decompressed_bytes);

    block_offsets.push_back(staged.size());
    block_compressed_sizes.push_back(compressed_bytes);
    block_decompressed_sizes.push_back(decompressed_bytes);
    staged.append(payload, compressed_bytes);
    staged_values_bytes += decompressed_bytes;
}

void GPUAccumulator::reduceBatchOnDevice()
{
    if (staged_values_bytes == 0)
        return;

    const size_t num_rows = staged_values_bytes / element_size;

    UInt64 batch_result = 0;
    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status = block_offsets.empty()
        ? clickhouseGPUReduce(
              element_type, result_type, aggregation, staged.data(), num_rows, &batch_result, error, sizeof(error))
        : clickhouseGPUReduceCompressed(
              *codec,
              element_type,
              result_type,
              aggregation,
              staged.data(),
              block_offsets.data(),
              block_compressed_sizes.data(),
              block_decompressed_sizes.data(),
              block_offsets.size(),
              num_rows,
              &batch_result,
              error,
              sizeof(error));
    const UInt64 elapsed_microseconds = watch.elapsedMicroseconds();

    if (status != 0)
        throw Exception(
            ErrorCodes::GPU_ERROR,
            "Cannot reduce {} values by `{}` on the device: {}",
            num_rows,
            aggregationName(aggregation),
            error);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, elapsed_microseconds);

    staged.clear();
    block_offsets.clear();
    block_compressed_sizes.clear();
    block_decompressed_sizes.clear();
    staged_values_bytes = 0;

    combine(batch_result);
}

void GPUAccumulator::combine(UInt64 batch_result)
{
    if (aggregation == CLICKHOUSE_GPU_AGGREGATION_SUM)
    {
        if (result_type == CLICKHOUSE_GPU_RESULT_FLOAT64)
            float_result += std::bit_cast<Float64>(batch_result);
        else
            integer_result += batch_result;

        has_result = true;
        return;
    }

    const bool take_smaller = aggregation == CLICKHOUSE_GPU_AGGREGATION_MIN;

    switch (result_type)
    {
        case CLICKHOUSE_GPU_RESULT_UINT64:
        {
            integer_result = !has_result
                ? batch_result
                : (take_smaller ? std::min(integer_result, batch_result) : std::max(integer_result, batch_result));
            break;
        }
        case CLICKHOUSE_GPU_RESULT_INT64:
        {
            const Int64 running = static_cast<Int64>(integer_result);
            const Int64 value = static_cast<Int64>(batch_result);
            integer_result = static_cast<UInt64>(
                !has_result ? value : (take_smaller ? std::min(running, value) : std::max(running, value)));
            break;
        }
        default:
        {
            const Float64 value = std::bit_cast<Float64>(batch_result);
            float_result = !has_result ? value : (take_smaller ? std::min(float_result, value) : std::max(float_result, value));
            break;
        }
    }

    has_result = true;
}

Field GPUAccumulator::finalize()
{
    reduceBatchOnDevice();

    switch (result_type)
    {
        case CLICKHOUSE_GPU_RESULT_UINT64:
            return Field(integer_result);
        case CLICKHOUSE_GPU_RESULT_INT64:
            return Field(static_cast<Int64>(integer_result));
        default:
            return Field(float_result);
    }
}

bool canGroupByReduceOnDevice(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<int> & aggregations)
{
    if (key_types.empty() || argument_types.empty() || argument_types.size() != result_types.size()
        || argument_types.size() != aggregations.size())
        return false;

    for (const auto & key_type : key_types)
    {
        const auto key_element_type = elementTypeOf(*key_type);
        if (!key_element_type)
            return false;

        if (*key_element_type == CLICKHOUSE_GPU_ELEMENT_FLOAT32 || *key_element_type == CLICKHOUSE_GPU_ELEMENT_FLOAT64)
            return false;
    }

    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!canReduceOnDevice(*argument_types[i], *result_types[i], aggregations[i]))
            return false;
    }

    return true;
}

namespace
{
template <typename T>
void * resizeAndGetValueBytes(IColumn & column, size_t num_rows)
{
    auto * vector = typeid_cast<ColumnVector<T> *>(&column);
    if (!vector)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot copy groups of {}-byte values out of the device into a column of {}",
            sizeof(T),
            column.getName());

    auto & data = vector->getData();
    if (!data.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cannot copy groups into a column of {} that already holds {} rows", column.getName(), data.size());

    data.resize(num_rows);
    return data.data();
}
}

void * resizeForElementType(IColumn & column, size_t num_rows, int element_type)
{
    switch (element_type)
    {
        case CLICKHOUSE_GPU_ELEMENT_UINT8: return resizeAndGetValueBytes<UInt8>(column, num_rows);
        case CLICKHOUSE_GPU_ELEMENT_UINT16: return resizeAndGetValueBytes<UInt16>(column, num_rows);
        case CLICKHOUSE_GPU_ELEMENT_UINT32: return resizeAndGetValueBytes<UInt32>(column, num_rows);
        case CLICKHOUSE_GPU_ELEMENT_UINT64: return resizeAndGetValueBytes<UInt64>(column, num_rows);
        case CLICKHOUSE_GPU_ELEMENT_INT8: return resizeAndGetValueBytes<Int8>(column, num_rows);
        case CLICKHOUSE_GPU_ELEMENT_INT16: return resizeAndGetValueBytes<Int16>(column, num_rows);
        case CLICKHOUSE_GPU_ELEMENT_INT32: return resizeAndGetValueBytes<Int32>(column, num_rows);
        case CLICKHOUSE_GPU_ELEMENT_INT64: return resizeAndGetValueBytes<Int64>(column, num_rows);
        case CLICKHOUSE_GPU_ELEMENT_FLOAT32: return resizeAndGetValueBytes<Float32>(column, num_rows);
        case CLICKHOUSE_GPU_ELEMENT_FLOAT64: return resizeAndGetValueBytes<Float64>(column, num_rows);
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU element type {}", element_type);
    }
}

namespace
{
void * resizeForResultType(IColumn & column, size_t num_rows, int result_type)
{
    switch (result_type)
    {
        case CLICKHOUSE_GPU_RESULT_UINT64: return resizeAndGetValueBytes<UInt64>(column, num_rows);
        case CLICKHOUSE_GPU_RESULT_INT64: return resizeAndGetValueBytes<Int64>(column, num_rows);
        case CLICKHOUSE_GPU_RESULT_FLOAT64: return resizeAndGetValueBytes<Float64>(column, num_rows);
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU result type {}", result_type);
    }
}

std::vector<int> keyElementTypesOrThrow(const DataTypes & key_types)
{
    std::vector<int> element_types;
    element_types.reserve(key_types.size());

    for (const auto & key_type : key_types)
    {
        const auto element_type = elementTypeOf(*key_type);
        if (!element_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot group by a column of {} on a GPU", key_type->getName());

        element_types.push_back(*element_type);
    }

    return element_types;
}

std::vector<int> valueElementTypesOrThrow(
    const DataTypes & argument_types, const DataTypes & result_types, const std::vector<int> & aggregations)
{
    std::vector<int> element_types;
    element_types.reserve(argument_types.size());

    for (size_t i = 0; i < argument_types.size(); ++i)
        element_types.push_back(elementTypeOrThrow(*argument_types[i], *result_types[i], aggregationOrThrow(aggregations[i])));

    return element_types;
}

std::vector<int> resultTypesOrThrow(const std::vector<int> & element_types)
{
    std::vector<int> result_types;
    result_types.reserve(element_types.size());

    for (const int element_type : element_types)
        result_types.push_back(resultTypeOrThrow(element_type));

    return result_types;
}

std::vector<size_t> elementSizesOf(const std::vector<int> & element_types)
{
    std::vector<size_t> sizes;
    sizes.reserve(element_types.size());

    for (const int element_type : element_types)
        sizes.push_back(elementSizeOf(element_type));

    return sizes;
}

size_t rowBytesOf(const std::vector<size_t> & key_element_sizes, const std::vector<size_t> & value_element_sizes)
{
    return std::accumulate(key_element_sizes.begin(), key_element_sizes.end(), size_t{0})
        + std::accumulate(value_element_sizes.begin(), value_element_sizes.end(), size_t{0});
}
}

GroupByGPUAccumulator::GroupByGPUAccumulator(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<int> & aggregations,
    size_t batch_bytes)
    : key_element_types(keyElementTypesOrThrow(key_types))
    , value_element_types(valueElementTypesOrThrow(argument_types, result_types, aggregations))
    , value_result_types(resultTypesOrThrow(value_element_types))
    , value_aggregations(aggregations)
    , key_element_sizes(elementSizesOf(key_element_types))
    , value_element_sizes(elementSizesOf(value_element_types))
    , batch_rows(std::clamp(batch_bytes / rowBytesOf(key_element_sizes, value_element_sizes), size_t{1}, max_batch_rows))
    , staged_keys(key_element_types.size())
    , staged_values(value_element_types.size())
{
    char error[error_buffer_size] = {};
    const int status = clickhouseGPUGroupByCreate(
        key_element_types.data(),
        key_element_types.size(),
        value_element_types.data(),
        value_result_types.data(),
        value_aggregations.data(),
        value_element_types.size(),
        &handle,
        error,
        sizeof(error));

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot group by {} keys on the device: {}", key_element_types.size(), error);

    for (size_t i = 0; i < staged_keys.size(); ++i)
        staged_keys[i].reserve(batch_rows * key_element_sizes[i]);
    for (size_t i = 0; i < staged_values.size(); ++i)
        staged_values[i].reserve(batch_rows * value_element_sizes[i]);
}

GroupByGPUAccumulator::~GroupByGPUAccumulator()
{
    clickhouseGPUGroupByDestroy(handle);
}

void GroupByGPUAccumulator::add(const Columns & key_columns, const Columns & value_columns)
{
    const size_t num_rows = key_columns.front()->size();
    if (num_rows == 0)
        return;

    if (staged_rows + num_rows > max_batch_rows)
        sendBatchToDevice();

    for (size_t i = 0; i < key_columns.size(); ++i)
    {
        const std::string_view raw = key_columns[i]->getRawData();
        staged_keys[i].append(raw.data(), raw.size());
    }

    for (size_t i = 0; i < value_columns.size(); ++i)
    {
        const std::string_view raw = value_columns[i]->getRawData();
        staged_values[i].append(raw.data(), raw.size());
    }

    staged_rows += num_rows;

    if (staged_rows >= batch_rows)
        sendBatchToDevice();
}

void GroupByGPUAccumulator::sendBatchToDevice()
{
    if (staged_rows == 0)
        return;

    std::vector<const void *> key_data(staged_keys.size());
    for (size_t i = 0; i < staged_keys.size(); ++i)
        key_data[i] = staged_keys[i].data();

    std::vector<const void *> value_data(staged_values.size());
    for (size_t i = 0; i < staged_values.size(); ++i)
        value_data[i] = staged_values[i].data();

    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status
        = clickhouseGPUGroupByAddBatch(handle, key_data.data(), value_data.data(), staged_rows, error, sizeof(error));
    const UInt64 elapsed_microseconds = watch.elapsedMicroseconds();

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot group {} rows on the device: {}", staged_rows, error);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, staged_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, elapsed_microseconds);

    for (auto & staged : staged_keys)
        staged.clear();
    for (auto & staged : staged_values)
        staged.clear();

    staged_rows = 0;
}

size_t GroupByGPUAccumulator::finalize()
{
    sendBatchToDevice();

    size_t groups = 0;
    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status = clickhouseGPUGroupByFinalize(handle, &groups, error, sizeof(error));
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot finalize a grouped aggregation on the device: {}", error);

    num_groups = groups;
    return groups;
}

void GroupByGPUAccumulator::copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns)
{
    std::vector<void *> key_data(key_columns.size());
    for (size_t i = 0; i < key_columns.size(); ++i)
        key_data[i] = resizeForElementType(*key_columns[i], *num_groups, key_element_types[i]);

    std::vector<void *> value_data(value_columns.size());
    for (size_t i = 0; i < value_columns.size(); ++i)
        value_data[i] = value_aggregations[i] == CLICKHOUSE_GPU_AGGREGATION_SUM
            ? resizeForResultType(*value_columns[i], *num_groups, value_result_types[i])
            : resizeForElementType(*value_columns[i], *num_groups, value_element_types[i]);

    if (*num_groups == 0)
        return;

    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status = clickhouseGPUGroupByCopyOut(handle, key_data.data(), value_data.data(), error, sizeof(error));
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot copy {} groups back from the device: {}", *num_groups, error);
}
}
}

#endif
