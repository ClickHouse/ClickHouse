#include <GPU/GPUAggregation.h>

#if USE_GPU

#include <GPU/GPUAggregationABI.h>

#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <bit>
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

    /// The result type has to be the one this argument's sum is asked for on the device. It is
    /// checked rather than assumed so that a change to what `sum` returns cannot quietly turn into
    /// a wrong answer here: the aggregation is then simply no longer eligible.
    const auto result_sum_type = sumTypeOf(result_type);
    return result_sum_type && result_sum_type == sumTypeFor(*element_type);
}

namespace
{

/// The types are `canSumOnDevice`'s to accept, and it is asked before an accumulator is built -
/// so reaching either of these is a mistake in the caller rather than an unsupported query.
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

SumAccumulator::SumAccumulator(const IDataType & argument_type, const IDataType & result_type, size_t batch_bytes_)
    : element_type(elementTypeOrThrow(argument_type, result_type))
    , sum_type(sumTypeOrThrow(result_type))
    , element_size(elementSizeOf(element_type))
    , batch_bytes(std::clamp(batch_bytes_, element_size, max_batch_rows * element_size))
{
}

void SumAccumulator::add(const IColumn & column)
{
    const std::string_view raw = column.getRawData();
    if (raw.size() != column.size() * element_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column {} of {} rows holds {} bytes of values, expected {}",
            column.getName(),
            column.size(),
            raw.size(),
            column.size() * element_size);

    if (staged.size() / element_size + column.size() > max_batch_rows)
        sumBatchOnDevice();

    staged.insert(raw.data(), raw.data() + raw.size());

    if (staged.size() >= batch_bytes)
        sumBatchOnDevice();
}

void SumAccumulator::sumBatchOnDevice()
{
    if (staged.empty())
        return;

    const size_t num_rows = staged.size() / element_size;

    /// Eight bytes for the device's answer, read back as whatever `sum_type` says below.
    UInt64 batch_sum = 0;
    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status
        = clickhouseGPUSum(element_type, sum_type, staged.data(), num_rows, &batch_sum, error, sizeof(error));
    const UInt64 elapsed_microseconds = watch.elapsedMicroseconds();

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot sum {} values on the device: {}", num_rows, error);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, elapsed_microseconds);

    staged.clear();

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
    /// A keyed aggregation with no keys is the keyless one, which `canSumOnDevice` and
    /// `SumAccumulator` are for.
    if (key_types.empty() || argument_types.empty() || argument_types.size() != result_types.size())
        return false;

    /// `elementTypeOf` switches on the outermost type, so `Nullable(UInt64)`,
    /// `LowCardinality(UInt64)` and every `Decimal` are turned away by it rather than by a check of
    /// their own - and so is anything else that is not one of the ten fixed-width numeric types.
    ///
    /// Floats are then turned away on top of that, for a reason that has nothing to do with what
    /// the device can compute and everything to do with what a group is. ClickHouse groups a
    /// `Float64` key by its eight bytes, so `0.0` and `-0.0` are two groups. cuDF's hash groupby
    /// compares float keys with `nan_equal_physical_equality_comparator`, which is IEEE equality
    /// with `NaN` equal to itself, so `0.0` and `-0.0` are one group - and two `NaN`s with
    /// different payloads are one group where ClickHouse makes two. Either difference turns into a
    /// different number of output rows, which is a wrong answer rather than a slower one.
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

/// The bytes of `column`'s values, which is what the device is given - checking on the way that the
/// column really is a run of `num_rows` values of `element_size` bytes each. A column that is
/// constant, sparse or low-cardinality is none of that, and the caller is the one that has to have
/// made it full.
std::string_view rawValuesOf(const IColumn & column, size_t num_rows, size_t element_size)
{
    if (column.size() != num_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column {} of a row set of {} rows has {} of its own",
            column.getName(),
            num_rows,
            column.size());

    const std::string_view raw = column.getRawData();
    if (raw.size() != num_rows * element_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column {} of {} rows holds {} bytes of values, expected {}",
            column.getName(),
            num_rows,
            raw.size(),
            num_rows * element_size);

    return raw;
}

/// Resizes `column` to `num_rows` and hands back the bytes its values occupy, so that the device
/// copies the groups straight into the column the query returns instead of into a staging buffer
/// that would then be copied again.
///
/// The type is checked rather than assumed: the caller supplies the columns, and one of a different
/// width would be filled with a shifted, meaningless run of bytes instead of failing.
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

    /// Leaves the values uninitialized, and every one of them is about to be overwritten by the
    /// copy from the device.
    data.resize(num_rows);
    return data.data();
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

/// How many bytes one row of the batch takes in host memory: every key and every value of it.
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
    /// The setting is in bytes and the cap is in rows, so the two meet here. Dividing rather than
    /// multiplying the cap keeps this from overflowing on a wide row set, and a batch is at least
    /// one row so that a tiny setting still makes progress.
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

    /// Checked before anything is staged, so that a mismatched row set leaves the batch as it was
    /// rather than half-appended.
    for (size_t i = 0; i < key_columns.size(); ++i)
        rawValuesOf(*key_columns[i], num_rows, key_element_sizes[i]);
    for (size_t i = 0; i < value_columns.size(); ++i)
        rawValuesOf(*value_columns[i], num_rows, value_element_sizes[i]);

    /// Sends what is staged before this row set would take the batch past what cuDF can count.
    if (staged_rows + num_rows > max_batch_rows)
        sendBatchToDevice();

    for (size_t i = 0; i < key_columns.size(); ++i)
    {
        const std::string_view raw = key_columns[i]->getRawData();
        staged_keys[i].insert(raw.data(), raw.data() + raw.size());
    }

    for (size_t i = 0; i < value_columns.size(); ++i)
    {
        const std::string_view raw = value_columns[i]->getRawData();
        staged_values[i].insert(raw.data(), raw.data() + raw.size());
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

    /// The batch is on the device and merged into the partial result there, so the host buffers are
    /// free for the next one. `clear` keeps their capacity, which is the point of staging at all:
    /// every batch after the first reuses the same host memory.
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

    /// Nothing was ever added, so there is nothing on the device to copy - and the pointers above
    /// point at columns of no rows, which is not something to hand over the boundary.
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
