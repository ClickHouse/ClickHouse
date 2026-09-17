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
/// Room for a message coming back over the boundary. cuDF's are a line or two.
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
/// What ClickHouse's own `sum` returns for such an argument: `UInt64` for any unsigned integer,
/// `Int64` for any signed one, `Float64` for both floats. The device is asked for exactly that
/// type, so a batch's sum needs no conversion on the way back.
std::optional<int> sumTypeFor(int element_type)
{
    switch (element_type)
    {
        case CLICKHOUSE_GPU_ELEMENT_UINT8:
        case CLICKHOUSE_GPU_ELEMENT_UINT16:
        case CLICKHOUSE_GPU_ELEMENT_UINT32:
        case CLICKHOUSE_GPU_ELEMENT_UINT64:
            return CLICKHOUSE_GPU_SUM_UINT64;
        case CLICKHOUSE_GPU_ELEMENT_INT8:
        case CLICKHOUSE_GPU_ELEMENT_INT16:
        case CLICKHOUSE_GPU_ELEMENT_INT32:
        case CLICKHOUSE_GPU_ELEMENT_INT64:
            return CLICKHOUSE_GPU_SUM_INT64;
        case CLICKHOUSE_GPU_ELEMENT_FLOAT32:
        case CLICKHOUSE_GPU_ELEMENT_FLOAT64:
            return CLICKHOUSE_GPU_SUM_FLOAT64;
        default:
            return {};
    }
}
}

std::optional<int> sumTypeOf(const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::UInt64: return CLICKHOUSE_GPU_SUM_UINT64;
        case TypeIndex::Int64: return CLICKHOUSE_GPU_SUM_INT64;
        case TypeIndex::Float64: return CLICKHOUSE_GPU_SUM_FLOAT64;
        default: return {};
    }
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

bool canSumOnDevice(const IDataType & argument_type, const IDataType & result_type)
{
    const auto element_type = elementTypeOf(argument_type);
    if (!element_type)
        return false;

    const auto result_sum_type = sumTypeOf(result_type);
    return result_sum_type && result_sum_type == sumTypeFor(*element_type);
}

namespace
{
int elementTypeOrThrow(const IDataType & argument_type, const IDataType & result_type)
{
    if (const auto element_type = elementTypeOf(argument_type); element_type && canSumOnDevice(argument_type, result_type))
        return *element_type;

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot sum a column of {} into {} on a GPU",
        argument_type.getName(),
        result_type.getName());
}

int sumTypeOrThrow(const IDataType & result_type)
{
    if (const auto sum_type = sumTypeOf(result_type))
        return *sum_type;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "A sum of {} cannot come back from a GPU", result_type.getName());
}
}

SumAccumulator::SumAccumulator(
    const IDataType & argument_type, const IDataType & result_type, size_t batch_bytes_, std::optional<int> codec_)
    : element_type(elementTypeOrThrow(argument_type, result_type))
    , sum_type(sumTypeOrThrow(result_type))
    , element_size(elementSizeOf(element_type))
    , batch_bytes(std::clamp(batch_bytes_, element_size, max_batch_rows * element_size))
    , codec(codec_)
{
    staged.reserve(batch_bytes);
}

void SumAccumulator::flushIfBatchWouldOverflow(size_t incoming_rows, size_t incoming_bytes)
{
    if (staged_values_bytes == 0)
        return;

    if (staged_values_bytes + incoming_bytes > batch_bytes
        || staged_values_bytes / element_size + incoming_rows > max_batch_rows)
        sumBatchOnDevice();
}

void SumAccumulator::add(const IColumn & column)
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
        sumBatchOnDevice();
}

void SumAccumulator::addBlock(const char * payload, size_t compressed_bytes, size_t decompressed_bytes)
{
    if (!codec)
        throw Exception(ErrorCodes::GPU_ERROR, "A compressed block needs a codec the device can expand");

    if (staged_values_bytes != 0 && block_offsets.empty())
        throw Exception(ErrorCodes::GPU_ERROR, "A batch of plain values cannot also take compressed blocks");

    if (decompressed_bytes % element_size != 0)
        throw Exception(
            ErrorCodes::GPU_ERROR,
            "A compressed block expands to {} bytes, which is not a whole number of {}-byte values",
            decompressed_bytes,
            element_size);

    flushIfBatchWouldOverflow(decompressed_bytes / element_size, decompressed_bytes);

    block_offsets.push_back(staged.size());
    block_compressed_sizes.push_back(compressed_bytes);
    block_decompressed_sizes.push_back(decompressed_bytes);
    staged.append(payload, compressed_bytes);
    staged_values_bytes += decompressed_bytes;
}

void SumAccumulator::sumBatchOnDevice()
{
    if (staged_values_bytes == 0)
        return;

    const size_t num_rows = staged_values_bytes / element_size;

    UInt64 batch_sum = 0;
    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status = block_offsets.empty()
        ? clickhouseGPUSum(element_type, sum_type, staged.data(), num_rows, &batch_sum, error, sizeof(error))
        : clickhouseGPUSumCompressed(
              *codec,
              element_type,
              sum_type,
              staged.data(),
              block_offsets.data(),
              block_compressed_sizes.data(),
              block_decompressed_sizes.data(),
              block_offsets.size(),
              num_rows,
              &batch_sum,
              error,
              sizeof(error));
    const UInt64 elapsed_microseconds = watch.elapsedMicroseconds();

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot sum {} values on the device: {}", num_rows, error);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, elapsed_microseconds);

    staged.clear();
    block_offsets.clear();
    block_compressed_sizes.clear();
    block_decompressed_sizes.clear();
    staged_values_bytes = 0;

    if (sum_type == CLICKHOUSE_GPU_SUM_FLOAT64)
        float_sum += std::bit_cast<Float64>(batch_sum);
    else
        integer_sum += batch_sum;
}

Field SumAccumulator::finalize()
{
    sumBatchOnDevice();

    switch (sum_type)
    {
        case CLICKHOUSE_GPU_SUM_UINT64:
            return Field(integer_sum);
        case CLICKHOUSE_GPU_SUM_INT64:
            return Field(static_cast<Int64>(integer_sum));
        default:
            return Field(float_sum);
    }
}

bool canGroupBySumOnDevice(const DataTypes & key_types, const DataTypes & argument_types, const DataTypes & result_types)
{
    if (key_types.empty() || argument_types.empty() || argument_types.size() != result_types.size())
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
        if (!canSumOnDevice(*argument_types[i], *result_types[i]))
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
void * resizeForSumType(IColumn & column, size_t num_rows, int sum_type)
{
    switch (sum_type)
    {
        case CLICKHOUSE_GPU_SUM_UINT64: return resizeAndGetValueBytes<UInt64>(column, num_rows);
        case CLICKHOUSE_GPU_SUM_INT64: return resizeAndGetValueBytes<Int64>(column, num_rows);
        case CLICKHOUSE_GPU_SUM_FLOAT64: return resizeAndGetValueBytes<Float64>(column, num_rows);
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU sum type {}", sum_type);
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

std::vector<int> valueElementTypesOrThrow(const DataTypes & argument_types, const DataTypes & result_types)
{
    if (argument_types.size() != result_types.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A grouped sum of {} arguments returning {} results",
            argument_types.size(),
            result_types.size());

    std::vector<int> element_types;
    element_types.reserve(argument_types.size());

    for (size_t i = 0; i < argument_types.size(); ++i)
        element_types.push_back(elementTypeOrThrow(*argument_types[i], *result_types[i]));

    return element_types;
}

std::vector<int> sumTypesOrThrow(const DataTypes & result_types)
{
    std::vector<int> sum_types;
    sum_types.reserve(result_types.size());

    for (const auto & result_type : result_types)
        sum_types.push_back(sumTypeOrThrow(*result_type));

    return sum_types;
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

GroupBySumAccumulator::GroupBySumAccumulator(
    const DataTypes & key_types, const DataTypes & argument_types, const DataTypes & result_types, size_t batch_bytes)
    : key_element_types(keyElementTypesOrThrow(key_types))
    , value_element_types(valueElementTypesOrThrow(argument_types, result_types))
    , value_sum_types(sumTypesOrThrow(result_types))
    , key_element_sizes(elementSizesOf(key_element_types))
    , value_element_sizes(elementSizesOf(value_element_types))
    , batch_rows(std::clamp(batch_bytes / rowBytesOf(key_element_sizes, value_element_sizes), size_t{1}, max_batch_rows))
    , staged_keys(key_element_types.size())
    , staged_values(value_element_types.size())
{
    char error[error_buffer_size] = {};
    const int status = clickhouseGPUGroupBySumCreate(
        key_element_types.data(),
        key_element_types.size(),
        value_element_types.data(),
        value_sum_types.data(),
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

GroupBySumAccumulator::~GroupBySumAccumulator()
{
    clickhouseGPUGroupBySumDestroy(handle);
}

void GroupBySumAccumulator::add(const Columns & key_columns, const Columns & value_columns)
{
    if (num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A row set was added to a grouped sum that was already finalized");

    if (key_columns.size() != staged_keys.size() || value_columns.size() != staged_values.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A row set of {} keys and {} values, where the grouped sum has {} and {}",
            key_columns.size(),
            value_columns.size(),
            staged_keys.size(),
            staged_values.size());

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

void GroupBySumAccumulator::sendBatchToDevice()
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
        = clickhouseGPUGroupBySumAddBatch(handle, key_data.data(), value_data.data(), staged_rows, error, sizeof(error));
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

size_t GroupBySumAccumulator::finalize()
{
    if (num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A grouped sum was finalized twice");

    sendBatchToDevice();

    size_t groups = 0;
    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status = clickhouseGPUGroupBySumFinalize(handle, &groups, error, sizeof(error));
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot finalize a grouped sum on the device: {}", error);

    num_groups = groups;
    return groups;
}

void GroupBySumAccumulator::copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a grouped sum were asked for before it was finalized");

    if (key_columns.size() != staged_keys.size() || value_columns.size() != staged_values.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Room for {} keys and {} sums, where the grouped sum has {} and {}",
            key_columns.size(),
            value_columns.size(),
            staged_keys.size(),
            staged_values.size());

    std::vector<void *> key_data(key_columns.size());
    for (size_t i = 0; i < key_columns.size(); ++i)
        key_data[i] = resizeForElementType(*key_columns[i], *num_groups, key_element_types[i]);

    std::vector<void *> value_data(value_columns.size());
    for (size_t i = 0; i < value_columns.size(); ++i)
        value_data[i] = resizeForSumType(*value_columns[i], *num_groups, value_sum_types[i]);

    if (*num_groups == 0)
        return;

    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status = clickhouseGPUGroupBySumCopyOut(handle, key_data.data(), value_data.data(), error, sizeof(error));
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot copy {} groups back from the device: {}", *num_groups, error);
}
}
}

#endif
