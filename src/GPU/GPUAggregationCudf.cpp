/// The cuDF side of the GPU engine - the aggregation and the hash join: everything here is compiled
/// by nvcc's host pass against the system libstdc++, not by ClickHouse's own toolchain, and it must
/// not include a ClickHouse header. It talks to the rest of the server only through the C boundary
/// declared in the header below - see that file, and `gpu_island_target` in cmake/cuda.cmake.
///
/// Both operators live in this one translation unit rather than in a file each, for two reasons.
/// They share the type mapping, the error convention and `checkNoNulls`, which would otherwise have
/// to move into a header of their own that only the island may include. And they share
/// `setUpDeviceMemoryResourceOnce`, whose `std::once_flag` has to be the one flag in the process: a
/// second copy of it in a second translation unit would replace RMM's current device resource while
/// buffers allocated from the first one were still alive.

#include <GPU/GPUAggregationABI.h>

#include <cudf/aggregation.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/groupby.hpp>
#include <cudf/join/hash_join.hpp>
#include <cudf/reduction.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>
#include <cudf/unary.hpp>
#include <cudf/utilities/default_stream.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_buffer.hpp>
#include <rmm/device_uvector.hpp>
#include <rmm/mr/cuda_async_memory_resource.hpp>
#include <rmm/mr/per_device_resource.hpp>

#include <cuda_runtime_api.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace
{

void writeError(char * error, size_t error_size, const std::string & message)
{
    if (error == nullptr || error_size == 0)
        return;

    const size_t length = std::min(message.size(), error_size - 1);
    std::memcpy(error, message.data(), length);
    error[length] = '\0';
}

/// One `cudaMallocAsync` pool for the whole process, in place of RMM's default resource, which
/// does a `cudaMalloc` and a `cudaFree` per allocation - and `cudaFree` synchronizes the device,
/// so with a batch of a few hundred megabytes it costs more than the reduction it feeds. The pool
/// hands the same pages back for the next batch instead. RMM keeps the resource alive until the
/// process exits and takes care of tearing it down there.
void setUpDeviceMemoryResourceOnce()
{
    static std::once_flag once;
    std::call_once(once, [] { rmm::mr::set_current_device_resource(rmm::mr::cuda_async_memory_resource{}); });
}

struct ElementLayout
{
    cudf::data_type type;
    size_t size;
};

ElementLayout elementLayoutOf(int element_type)
{
    switch (element_type)
    {
        case CLICKHOUSE_GPU_ELEMENT_UINT8: return {cudf::data_type{cudf::type_id::UINT8}, 1};
        case CLICKHOUSE_GPU_ELEMENT_UINT16: return {cudf::data_type{cudf::type_id::UINT16}, 2};
        case CLICKHOUSE_GPU_ELEMENT_UINT32: return {cudf::data_type{cudf::type_id::UINT32}, 4};
        case CLICKHOUSE_GPU_ELEMENT_UINT64: return {cudf::data_type{cudf::type_id::UINT64}, 8};
        case CLICKHOUSE_GPU_ELEMENT_INT8: return {cudf::data_type{cudf::type_id::INT8}, 1};
        case CLICKHOUSE_GPU_ELEMENT_INT16: return {cudf::data_type{cudf::type_id::INT16}, 2};
        case CLICKHOUSE_GPU_ELEMENT_INT32: return {cudf::data_type{cudf::type_id::INT32}, 4};
        case CLICKHOUSE_GPU_ELEMENT_INT64: return {cudf::data_type{cudf::type_id::INT64}, 8};
        case CLICKHOUSE_GPU_ELEMENT_FLOAT32: return {cudf::data_type{cudf::type_id::FLOAT32}, 4};
        case CLICKHOUSE_GPU_ELEMENT_FLOAT64: return {cudf::data_type{cudf::type_id::FLOAT64}, 8};
        default: throw std::logic_error("unknown element type " + std::to_string(element_type));
    }
}

cudf::data_type sumTypeOf(int sum_type)
{
    switch (sum_type)
    {
        case CLICKHOUSE_GPU_SUM_UINT64: return cudf::data_type{cudf::type_id::UINT64};
        case CLICKHOUSE_GPU_SUM_INT64: return cudf::data_type{cudf::type_id::INT64};
        case CLICKHOUSE_GPU_SUM_FLOAT64: return cudf::data_type{cudf::type_id::FLOAT64};
        default: throw std::logic_error("unknown sum type " + std::to_string(sum_type));
    }
}

/// Reads the reduction's result out of the scalar and into the eight bytes the caller owns.
void writeSum(const cudf::scalar & sum, int sum_type, void * result, rmm::cuda_stream_view stream)
{
    if (!sum.is_valid(stream))
        throw std::runtime_error("the device returned no sum for a non-empty batch of values without nulls");

    /// The reduction was asked for this type, so a different one means cuDF and the switch below
    /// disagree about what a sum is - and the casts below would be reading the wrong bytes.
    if (sum.type() != sumTypeOf(sum_type))
        throw std::logic_error(
            "the device returned a sum of type " + std::to_string(static_cast<int32_t>(sum.type().id()))
            + ", expected " + std::to_string(static_cast<int32_t>(sumTypeOf(sum_type).id())));

    switch (sum_type)
    {
        case CLICKHOUSE_GPU_SUM_UINT64:
        {
            const auto value = static_cast<const cudf::numeric_scalar<uint64_t> &>(sum).value(stream);
            std::memcpy(result, &value, sizeof(value));
            return;
        }
        case CLICKHOUSE_GPU_SUM_INT64:
        {
            const auto value = static_cast<const cudf::numeric_scalar<int64_t> &>(sum).value(stream);
            std::memcpy(result, &value, sizeof(value));
            return;
        }
        case CLICKHOUSE_GPU_SUM_FLOAT64:
        {
            const auto value = static_cast<const cudf::numeric_scalar<double> &>(sum).value(stream);
            std::memcpy(result, &value, sizeof(value));
            return;
        }
        default:
            throw std::logic_error("unknown sum type " + std::to_string(sum_type));
    }
}


/// Every sum this boundary carries is eight bytes wide, whichever of the three `ClickHouseGPUSumType`
/// it is - which is why copying a partial-sum column out is a plain memcpy of `num_groups * 8`.
constexpr size_t sum_type_size = 8;

/// The type `cudf::groupby`'s `SUM` produces for a column of `source`.
///
/// Unlike `cudf::reduce`, which is told the type to reduce into, the groupby has no output-type
/// parameter at all: `cudf::detail::target_type_t` decides, and it accumulates any integral column
/// in an `int64_t` and a floating point column in its own type. So this is not a preference, it is
/// what cuDF will hand back, and the code below arranges the input so that what it hands back is
/// what ClickHouse asked for.
cudf::data_type cudfGroupBySumTargetTypeFor(cudf::data_type source)
{
    switch (source.id())
    {
        case cudf::type_id::UINT8:
        case cudf::type_id::UINT16:
        case cudf::type_id::UINT32:
        case cudf::type_id::UINT64:
        case cudf::type_id::INT8:
        case cudf::type_id::INT16:
        case cudf::type_id::INT32:
        case cudf::type_id::INT64:
            return cudf::data_type{cudf::type_id::INT64};
        case cudf::type_id::FLOAT32:
            return cudf::data_type{cudf::type_id::FLOAT32};
        case cudf::type_id::FLOAT64:
            return cudf::data_type{cudf::type_id::FLOAT64};
        default:
            throw std::logic_error(
                "cuDF type " + std::to_string(static_cast<int32_t>(source.id())) + " is not one this path groups by or sums");
    }
}

/// The type the partial sums are held in on the device, for a sum ClickHouse wants as `sum_type`.
///
/// `CLICKHOUSE_GPU_SUM_UINT64` comes out as a signed `INT64` because cuDF's groupby has no unsigned
/// sum - see `cudfGroupBySumTargetTypeFor`. That is not a loss: two's complement addition wraps
/// modulo 2^64 whether the operand is read as signed or unsigned, so the eight bytes are the same
/// eight bytes, and ClickHouse's own `sum` over an unsigned column wraps modulo 2^64 as well. The
/// ClickHouse side reads them back as a `UInt64`.
cudf::data_type deviceSumTypeOf(int sum_type)
{
    switch (sum_type)
    {
        case CLICKHOUSE_GPU_SUM_UINT64:
        case CLICKHOUSE_GPU_SUM_INT64:
            return cudf::data_type{cudf::type_id::INT64};
        case CLICKHOUSE_GPU_SUM_FLOAT64:
            return cudf::data_type{cudf::type_id::FLOAT64};
        default:
            throw std::logic_error("unknown sum type " + std::to_string(sum_type));
    }
}

/// A column produced by the device that the host is about to read, or that a later groupby is about
/// to read as a value: a null in it would mean the bytes underneath are undefined.
///
/// Nulls cannot arise on this path - the views handed to cuDF carry no null mask, and `SUM` returns
/// a null only for a group with no non-null value - so this is an invariant check rather than a
/// case to handle. cuDF is free to attach an all-valid mask to an output, which is why the test is
/// on `null_count` rather than on `nullable`.
void checkNoNulls(const cudf::column_view & column, const std::string & what)
{
    if (column.null_count() != 0)
        throw std::logic_error(
            "the device returned " + std::to_string(column.null_count()) + " nulls in " + what
            + ", where the input had no null mask at all");
}

/// Groups the rows of `keys` and sums each of `values` within every group. Returns one table: the
/// distinct key rows first, in the order `keys` gives their columns, then one partial sum per value
/// column, in the order `values` gives them - the same shape this function takes, which is what
/// lets its own output be fed back into it when partial results are merged.
///
/// `sum_types` says what each sum has to come back as, and is checked rather than assumed: the
/// eight bytes eventually copied out are read as that type on the ClickHouse side, so cuDF
/// returning a different one would silently be a wrong answer instead of an error.
std::unique_ptr<cudf::table> groupBySum(
    const cudf::table_view & keys,
    const std::vector<cudf::column_view> & values,
    const std::vector<cudf::data_type> & sum_types,
    rmm::cuda_stream_view stream)
{
    std::vector<cudf::groupby::aggregation_request> requests;
    requests.reserve(values.size());

    for (size_t i = 0; i < values.size(); ++i)
    {
        /// The value column is already of the type its sum comes back as, either because it was
        /// uploaded or cast to it, or because it is itself a partial sum from an earlier groupby.
        /// cuDF's target type for such a column is that same type - `INT64` sums into `INT64`,
        /// `FLOAT64` into `FLOAT64` - so the merge below sums a column of the sum type into itself
        /// and the partial result keeps its shape however many times it is merged.
        if (cudfGroupBySumTargetTypeFor(values[i].type()) != sum_types[i])
            throw std::logic_error(
                "a groupby over a value column of cuDF type " + std::to_string(static_cast<int32_t>(values[i].type().id()))
                + " sums into type " + std::to_string(static_cast<int32_t>(cudfGroupBySumTargetTypeFor(values[i].type()).id()))
                + ", not into the expected " + std::to_string(static_cast<int32_t>(sum_types[i].id())));

        cudf::groupby::aggregation_request request;
        request.values = values[i];
        request.aggregations.push_back(cudf::make_sum_aggregation<cudf::groupby_aggregation>());
        requests.push_back(std::move(request));
    }

    /// `null_policy::EXCLUDE` and `null_policy::INCLUDE` differ only in whether a key row holding a
    /// null forms a group of its own, and there are no nulls to decide about: nullable keys are not
    /// eligible for this path, and the views built by the callers carry no null mask. The default is
    /// left in place so that whoever makes nullable keys eligible has to choose deliberately -
    /// ClickHouse groups `NULL` with `NULL`, which is `INCLUDE`.
    cudf::groupby::groupby grouper(keys, cudf::null_policy::EXCLUDE);

    auto [group_keys, results] = grouper.aggregate(requests, stream);

    std::vector<std::unique_ptr<cudf::column>> columns = group_keys->release();

    for (size_t i = 0; i < results.size(); ++i)
    {
        if (results[i].results.size() != 1)
            throw std::logic_error(
                "the device returned " + std::to_string(results[i].results.size()) + " results for one requested sum");

        std::unique_ptr<cudf::column> & sum = results[i].results.front();

        if (sum->type() != sum_types[i])
            throw std::logic_error(
                "the device returned a sum of cuDF type " + std::to_string(static_cast<int32_t>(sum->type().id()))
                + ", expected " + std::to_string(static_cast<int32_t>(sum_types[i].id())));

        checkNoNulls(sum->view(), "a column of partial sums");
        columns.push_back(std::move(sum));
    }

    return std::make_unique<cudf::table>(std::move(columns));
}

/// The partial result of one keyed `sum`, living on the device between batches. What the handle
/// `clickhouseGPUGroupBySumCreate` returns points at.
struct GroupBySumState
{
    /// How the key columns arrive from the host, and how they go back: a group's key is one of the
    /// input keys, so a key column keeps its type all the way through.
    std::vector<ElementLayout> keys;

    /// How the value columns arrive from the host, and what their sums are held in.
    std::vector<ElementLayout> values;
    std::vector<cudf::data_type> sum_types;

    /// The groups so far: the key columns first, then one partial sum per value column - the shape
    /// `groupBySum` both produces and consumes. Null until the first batch, which is also how no
    /// rows at all is represented.
    std::unique_ptr<cudf::table> partial;

    bool finalized = false;
};

/// Whether `element_type` is one of the eight integer types. The join's key is restricted to those
/// because ClickHouse compares a key by its bytes while cuDF compares a float key by IEEE equality,
/// which disagree about `0.0` against `-0.0` and about two `NaN`s - the same reason floats are not
/// eligible as `GROUP BY` keys, and with the same consequence: a different number of output rows,
/// which is a wrong answer rather than a slower one.
bool isIntegerElementType(int element_type)
{
    switch (element_type)
    {
        case CLICKHOUSE_GPU_ELEMENT_UINT8:
        case CLICKHOUSE_GPU_ELEMENT_UINT16:
        case CLICKHOUSE_GPU_ELEMENT_UINT32:
        case CLICKHOUSE_GPU_ELEMENT_UINT64:
        case CLICKHOUSE_GPU_ELEMENT_INT8:
        case CLICKHOUSE_GPU_ELEMENT_INT16:
        case CLICKHOUSE_GPU_ELEMENT_INT32:
        case CLICKHOUSE_GPU_ELEMENT_INT64:
            return true;
        default:
            return false;
    }
}

/// cuDF counts the rows of a column in a signed 32-bit integer, so nothing handed to it may hold
/// more rows than that.
void checkRowCountFitsCudf(size_t num_rows, const std::string & what)
{
    if (num_rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
        throw std::logic_error(what + " of " + std::to_string(num_rows) + " rows is too large for cuDF");
}

/// Appends `bytes` of host memory to the end of a device buffer, growing it by doubling.
///
/// Doubling rather than resizing to fit: `rmm::device_buffer::resize` past the capacity allocates
/// and copies everything accumulated so far, so a right table arriving in 65536-row blocks would
/// pay that copy once per block - quadratic in the size of the table. Doubling makes the total
/// copying linear, at the cost of up to twice the device memory the build side needs.
void appendToDeviceBuffer(rmm::device_buffer & buffer, const void * host_data, size_t bytes, rmm::cuda_stream_view stream)
{
    const size_t old_size = buffer.size();
    const size_t new_size = old_size + bytes;

    if (new_size > buffer.capacity())
        buffer.reserve(std::max(new_size, buffer.capacity() * 2), stream);

    buffer.resize(new_size, stream);

    if (const cudaError_t status = cudaMemcpyAsync(
            static_cast<char *>(buffer.data()) + old_size, host_data, bytes, cudaMemcpyHostToDevice, stream.value());
        status != cudaSuccess)
        throw std::runtime_error(std::string("cannot copy a column of the right table to the device: ") + cudaGetErrorString(status));
}

/// The build side of one join, the hash table over it and the last probe's result - all on the
/// device. What the handle `clickhouseGPUHashJoinCreate` returns points at.
struct HashJoinState
{
    /// How the key column arrives, on both sides of the join: the two key types are the same, which
    /// is what lets one `ElementLayout` describe both.
    ElementLayout key;

    /// How the build side's payload columns arrive, and how they go back - a gathered payload keeps
    /// the type it was uploaded with.
    std::vector<ElementLayout> payloads;

    /// The right table as it arrives, block by block, with nothing staged in host memory in
    /// between.
    rmm::device_buffer build_keys;
    std::vector<rmm::device_buffer> build_payloads;
    size_t build_rows = 0;

    /// Views over the buffers above and the hash table over the keys, all made by
    /// `clickhouseGPUHashJoinFinishBuild`. From then on the buffers may not grow: `cudf::hash_join`
    /// "must not outlive the table viewed by `right`" and reads through this view on every probe, so
    /// a reallocation would leave it pointing at freed memory. That is why the build is a phase of
    /// its own and not something a probe can be interleaved with.
    cudf::table_view build_keys_table;
    cudf::table_view build_payloads_table;
    std::unique_ptr<cudf::hash_join> hash_table;
    bool built = false;

    /// The last probe's result. `probe_indices` holds one probe-side row index per matching pair of
    /// rows, and `gathered_payloads` the build side's payload columns already gathered at the
    /// matching build rows - so what the host reads back is the join's output and never the right
    /// table itself.
    ///
    /// Declared after `hash_table` and the buffers on purpose: members are destroyed in reverse, so
    /// the result goes first, then the hash table, and only then the build side it views.
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> probe_indices;
    std::unique_ptr<cudf::table> gathered_payloads;
    bool has_result = false;
};

}

extern "C"
{

int clickhouseGPUProbeDevice(char * error, size_t error_size)
{
    int count = 0;
    if (const cudaError_t status = cudaGetDeviceCount(&count); status != cudaSuccess)
    {
        writeError(error, error_size, std::string("cudaGetDeviceCount: ") + cudaGetErrorString(status));
        return 1;
    }

    if (count == 0)
    {
        writeError(error, error_size, "there is no CUDA device");
        return 1;
    }

    /// Brings the context up, so that a driver which is installed but cannot be used says so here
    /// instead of in the middle of the first query that reaches the device.
    if (const cudaError_t status = cudaFree(nullptr); status != cudaSuccess)
    {
        writeError(error, error_size, std::string("cannot initialize a CUDA context: ") + cudaGetErrorString(status));
        return 1;
    }

    return 0;
}

int clickhouseGPUSum(
    int element_type,
    int sum_type,
    const void * host_data,
    size_t num_rows,
    void * result,
    char * error,
    size_t error_size)
{
    try
    {
        if (num_rows == 0)
            throw std::logic_error("nothing to sum");

        /// cuDF counts rows in a signed 32-bit type, so a batch has to stay under two billion
        /// rows. The caller batches by bytes and the smallest element is one byte wide, so this
        /// bounds the batch at 2 GB for `Int8` and proportionally more for wider types.
        if (num_rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
            throw std::logic_error("a batch of " + std::to_string(num_rows) + " rows is too large for cuDF");

        const ElementLayout element = elementLayoutOf(element_type);
        const cudf::data_type sum_data_type = sumTypeOf(sum_type);

        setUpDeviceMemoryResourceOnce();

        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        /// The one transfer of the batch, and the reduction over it. Nothing else here touches
        /// host memory, which is the whole point of batching on the way in: this copy runs at the
        /// speed of the link, and the reduction at the speed of the device's own memory.
        const rmm::device_buffer device_data(host_data, num_rows * element.size, stream);

        const cudf::column_view column(
            element.type, static_cast<cudf::size_type>(num_rows), device_data.data(), nullptr, 0);

        const auto aggregation = cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        const auto sum = cudf::reduce(column, *aggregation, sum_data_type, stream);

        writeSum(*sum, sum_type, result, stream);
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything: nothing may leave this function as an exception, and every
    /// exception that reaches here does leave as a message the caller turns back into one.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

int clickhouseGPUGroupBySumCreate(
    const int * key_element_types,
    size_t num_keys,
    const int * value_element_types,
    const int * value_sum_types,
    size_t num_values,
    void ** handle,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("nowhere to put the handle");

        *handle = nullptr;

        if (num_keys == 0)
            throw std::logic_error("a keyed aggregation with no keys");
        if (num_values == 0)
            throw std::logic_error("a keyed aggregation with nothing to sum");

        auto state = std::make_unique<GroupBySumState>();

        state->keys.reserve(num_keys);
        for (size_t i = 0; i < num_keys; ++i)
            state->keys.push_back(elementLayoutOf(key_element_types[i]));

        state->values.reserve(num_values);
        state->sum_types.reserve(num_values);
        for (size_t i = 0; i < num_values; ++i)
        {
            state->values.push_back(elementLayoutOf(value_element_types[i]));
            state->sum_types.push_back(deviceSumTypeOf(value_sum_types[i]));
        }

        /// Brought up here rather than on the first batch so that a machine which cannot set up the
        /// pool says so before any data has been staged for it.
        setUpDeviceMemoryResourceOnce();

        *handle = state.release();
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything: nothing may leave this function as an exception, and every
    /// exception that reaches here does leave as a message the caller turns back into one.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

int clickhouseGPUGroupBySumAddBatch(
    void * handle,
    const void * const * key_host_data,
    const void * const * value_host_data,
    size_t num_rows,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no handle");

        GroupBySumState & state = *static_cast<GroupBySumState *>(handle);

        if (state.finalized)
            throw std::logic_error("a batch added after the partial result was finalized");

        if (num_rows == 0)
            throw std::logic_error("nothing to group");

        /// cuDF counts rows in a signed 32-bit type, so a batch has to stay under two billion rows.
        /// The caller batches by bytes and caps the row count at exactly this, so reaching here
        /// means the two sides disagree about the cap rather than that a query is too large.
        if (num_rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
            throw std::logic_error("a batch of " + std::to_string(num_rows) + " rows is too large for cuDF");

        const auto batch_rows = static_cast<cudf::size_type>(num_rows);
        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        /// The buffers own the uploaded batch and the views only point into them, so both have to
        /// outlive the groupby below - hence the two parallel vectors rather than a view built on
        /// the spot. This is the one transfer of the batch; everything after it stays on the device.
        std::vector<rmm::device_buffer> key_buffers;
        std::vector<cudf::column_view> key_views;
        key_buffers.reserve(state.keys.size());
        key_views.reserve(state.keys.size());

        for (size_t i = 0; i < state.keys.size(); ++i)
        {
            key_buffers.emplace_back(key_host_data[i], num_rows * state.keys[i].size, stream);
            key_views.emplace_back(state.keys[i].type, batch_rows, key_buffers.back().data(), nullptr, 0);
        }

        std::vector<rmm::device_buffer> value_buffers;
        /// Only the value columns that need widening (see below) put anything here; the vector
        /// exists to keep those columns alive for as long as the views over them.
        std::vector<std::unique_ptr<cudf::column>> widened_values;
        std::vector<cudf::column_view> value_views;
        value_buffers.reserve(state.values.size());
        value_views.reserve(state.values.size());

        for (size_t i = 0; i < state.values.size(); ++i)
        {
            value_buffers.emplace_back(value_host_data[i], num_rows * state.values[i].size, stream);

            const cudf::column_view uploaded(
                state.values[i].type, batch_rows, value_buffers.back().data(), nullptr, 0);

            if (cudfGroupBySumTargetTypeFor(uploaded.type()) == state.sum_types[i])
            {
                value_views.push_back(uploaded);
                continue;
            }

            /// cuDF's groupby sums a column into a type of its own choosing, and for one case that
            /// is not the type ClickHouse's `sum` returns: a `Float32` column sums into a `Float32`,
            /// where ClickHouse gives a `Float64`. So widen the column first and let the groupby
            /// sum `Float64` into `Float64`. The widening is done here, on the device, rather than
            /// while staging on the host, because the host copy is what crosses the link and there
            /// is no reason to send twice the bytes. Integral columns need none of this - cuDF
            /// widens every one of them to `INT64` by itself.
            widened_values.push_back(cudf::cast(uploaded, state.sum_types[i], stream));
            value_views.push_back(widened_values.back()->view());
        }

        std::unique_ptr<cudf::table> batch
            = groupBySum(cudf::table_view(key_views), value_views, state.sum_types, stream);

        if (!state.partial)
        {
            state.partial = std::move(batch);
            return 0;
        }

        /// The merge. `sum` is associative, and a partial sum per group has the same shape as the
        /// batch's own result, so stacking the two partial results on top of each other and
        /// grouping that gives exactly the groups and sums one groupby over every row would have:
        /// each group's rows in the concatenation are the partial sums of that group's rows in the
        /// batches, and summing those sums is summing the rows. Which is why the merge can be the
        /// same `groupBySum` again and needs no separate merge operator.
        ///
        /// It also keeps the partial result on the device, unlike merging on the host, and its cost
        /// is proportional to the number of groups rather than to the number of rows seen so far.
        const std::vector<cudf::table_view> to_concatenate{state.partial->view(), batch->view()};
        const std::unique_ptr<cudf::table> concatenated = cudf::concatenate(to_concatenate, stream);

        std::vector<cudf::size_type> key_indices(state.keys.size());
        for (size_t i = 0; i < state.keys.size(); ++i)
            key_indices[i] = static_cast<cudf::size_type>(i);

        std::vector<cudf::column_view> partial_sum_views;
        partial_sum_views.reserve(state.values.size());
        for (size_t i = 0; i < state.values.size(); ++i)
            partial_sum_views.push_back(concatenated->view().column(static_cast<cudf::size_type>(state.keys.size() + i)));

        state.partial = groupBySum(concatenated->select(key_indices), partial_sum_views, state.sum_types, stream);
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything, for the reason given above.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

int clickhouseGPUGroupBySumFinalize(void * handle, size_t * num_groups, char * error, size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no handle");
        if (num_groups == nullptr)
            throw std::logic_error("nowhere to put the number of groups");

        GroupBySumState & state = *static_cast<GroupBySumState *>(handle);

        state.finalized = true;

        /// No batch was ever added, so the aggregation read no rows and has no groups. There is no
        /// row to produce for the empty input the way a keyless aggregation has one.
        *num_groups = state.partial ? static_cast<size_t>(state.partial->num_rows()) : 0;
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything, for the reason given above.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

int clickhouseGPUGroupBySumCopyOut(
    void * handle,
    void * const * key_host_data,
    void * const * value_host_data,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no handle");

        GroupBySumState & state = *static_cast<GroupBySumState *>(handle);

        if (!state.finalized)
            throw std::logic_error("the groups were copied out before the partial result was finalized");

        if (!state.partial)
            return 0;

        const rmm::cuda_stream_view stream = cudf::get_default_stream();
        const cudf::table_view groups = state.partial->view();
        const auto num_groups = static_cast<size_t>(groups.num_rows());

        const auto copyColumnOut = [&](const cudf::column_view & column, void * destination, size_t element_size, const std::string & what)
        {
            checkNoNulls(column, what);

            /// These columns come straight out of a groupby, which allocates each of them for
            /// itself, so none of them is a slice of a larger one. `head` would be the wrong pointer
            /// if one were, and there is no `offset` to add to a `void *`.
            if (column.offset() != 0)
                throw std::logic_error("the device returned " + what + " as a slice at offset " + std::to_string(column.offset()));

            if (const cudaError_t status = cudaMemcpyAsync(
                    destination, column.head<void>(), num_groups * element_size, cudaMemcpyDeviceToHost, stream.value());
                status != cudaSuccess)
                throw std::runtime_error("cannot copy " + what + " back: " + cudaGetErrorString(status));
        };

        for (size_t i = 0; i < state.keys.size(); ++i)
            copyColumnOut(
                groups.column(static_cast<cudf::size_type>(i)), key_host_data[i], state.keys[i].size, "a column of group keys");

        for (size_t i = 0; i < state.values.size(); ++i)
            copyColumnOut(
                groups.column(static_cast<cudf::size_type>(state.keys.size() + i)),
                value_host_data[i],
                sum_type_size,
                "a column of sums");

        /// The copies were queued on the stream, so the host memory only holds the groups once it
        /// has run. Synchronizing once, after all of the columns, rather than once per column.
        stream.synchronize();
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything, for the reason given above.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

void clickhouseGPUGroupBySumDestroy(void * handle)
{
    /// `delete` runs `~table`, which frees the device memory through the same memory resource that
    /// allocated it, and neither that nor `~vector` throws - so there is nothing here to catch and
    /// nothing that could be reported anyway.
    delete static_cast<GroupBySumState *>(handle);
}

int clickhouseGPUHashJoinCreate(
    int key_element_type,
    const int * payload_element_types,
    size_t num_payloads,
    void ** handle,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("nowhere to put the handle");

        *handle = nullptr;

        if (!isIntegerElementType(key_element_type))
            throw std::logic_error("a join key of element type " + std::to_string(key_element_type) + " is not an integer");

        auto state = std::make_unique<HashJoinState>();

        state->key = elementLayoutOf(key_element_type);

        state->payloads.reserve(num_payloads);
        for (size_t i = 0; i < num_payloads; ++i)
            state->payloads.push_back(elementLayoutOf(payload_element_types[i]));

        state->build_payloads.resize(num_payloads);

        /// Brought up here rather than on the first block so that a machine which cannot set up the
        /// pool says so before any data has been sent to it.
        setUpDeviceMemoryResourceOnce();

        *handle = state.release();
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything: nothing may leave this function as an exception, and every
    /// exception that reaches here does leave as a message the caller turns back into one.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

int clickhouseGPUHashJoinAddBuildBlock(
    void * handle,
    const void * key_host_data,
    const void * const * payload_host_data,
    size_t num_rows,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no handle");

        HashJoinState & state = *static_cast<HashJoinState *>(handle);

        if (state.built)
            throw std::logic_error("a block of the right table arrived after the hash table was built");
        if (num_rows == 0)
            throw std::logic_error("an empty block of the right table");

        checkRowCountFitsCudf(state.build_rows + num_rows, "a right table");

        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        appendToDeviceBuffer(state.build_keys, key_host_data, num_rows * state.key.size, stream);
        for (size_t i = 0; i < state.payloads.size(); ++i)
            appendToDeviceBuffer(state.build_payloads[i], payload_host_data[i], num_rows * state.payloads[i].size, stream);

        state.build_rows += num_rows;

        /// The source of every copy above is one of the caller's own columns, and the caller
        /// releases the block as soon as this returns - so the copies have to be done reading it by
        /// then, and only a synchronize says they are. CUDA happens to make this true already for
        /// pageable host memory, which it stages through a pinned buffer before returning, but
        /// leaning on that would tie correctness to the columns never being pinned. A synchronize
        /// per block costs microseconds against a transfer of half a megabyte and up.
        stream.synchronize();
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything, for the reason given above.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

int clickhouseGPUHashJoinFinishBuild(void * handle, char * error, size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no handle");

        HashJoinState & state = *static_cast<HashJoinState *>(handle);

        if (state.built)
            throw std::logic_error("the hash table was built twice");

        state.built = true;

        const auto build_rows = static_cast<cudf::size_type>(state.build_rows);

        /// The views the hash table and every gather read through. They are stored rather than
        /// rebuilt per probe because `cudf::table_view` holds copies of the column views, so the
        /// hash table's reference has to be to a view that lives as long as it does.
        const std::vector<cudf::column_view> key_views{
            cudf::column_view(state.key.type, build_rows, state.build_keys.data(), nullptr, 0)};
        state.build_keys_table = cudf::table_view(key_views);

        std::vector<cudf::column_view> payload_views;
        payload_views.reserve(state.payloads.size());
        for (size_t i = 0; i < state.payloads.size(); ++i)
            payload_views.emplace_back(state.payloads[i].type, build_rows, state.build_payloads[i].data(), nullptr, 0);
        state.build_payloads_table = cudf::table_view(payload_views);

        /// An inner join whose right table has no rows has an empty result whatever the left table
        /// holds, so there is nothing to build a hash table over and nothing a probe could find.
        /// Leaving it unbuilt is not a fallback to some other way of computing the join - it is this
        /// join's answer for the one input whose answer does not depend on the data.
        if (state.build_rows == 0)
            return 0;

        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        /// `null_equality::EQUAL` is what the aggregation's groupby leaves at its default for the
        /// same reason: there are no nulls to decide about, since nullable keys are not eligible for
        /// this path and the view above carries no null mask. Whoever makes them eligible has to
        /// choose deliberately - ClickHouse's `JOIN` does not match `NULL` to `NULL`, which is
        /// `null_equality::UNEQUAL`.
        state.hash_table = std::make_unique<cudf::hash_join>(state.build_keys_table, cudf::null_equality::EQUAL, stream);
        stream.synchronize();
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything, for the reason given above.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

int clickhouseGPUHashJoinProbe(
    void * handle,
    const void * key_host_data,
    size_t num_rows,
    size_t * num_matches,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no handle");
        if (num_matches == nullptr)
            throw std::logic_error("nowhere to put the number of matches");

        *num_matches = 0;

        HashJoinState & state = *static_cast<HashJoinState *>(handle);

        if (!state.built)
            throw std::logic_error("a probe before the hash table was built");
        if (num_rows == 0)
            throw std::logic_error("an empty block of the left table");

        checkRowCountFitsCudf(num_rows, "a left block");

        /// The previous probe's result is released before this one runs, so that two results are
        /// never on the device at once. It is also what makes a failed probe leave no stale result
        /// behind for the copy out to read.
        state.gathered_payloads.reset();
        state.probe_indices.reset();
        state.has_result = false;

        /// Nothing to probe: the right table had no rows, so the result is empty. See
        /// `clickhouseGPUHashJoinFinishBuild`.
        if (!state.hash_table)
        {
            state.has_result = true;
            return 0;
        }

        const rmm::cuda_stream_view stream = cudf::get_default_stream();
        const auto probe_rows = static_cast<cudf::size_type>(num_rows);

        /// The one transfer this probe pays for, and the only part of the left block that crosses
        /// the link: its key column. The left block's payload stays on the host and is indexed there
        /// with the row indices this returns.
        const rmm::device_buffer probe_keys(key_host_data, num_rows * state.key.size, stream);
        const std::vector<cudf::column_view> probe_key_views{
            cudf::column_view(state.key.type, probe_rows, probe_keys.data(), nullptr, 0)};

        auto [probe_indices, build_indices]
            = state.hash_table->inner_join(cudf::table_view(probe_key_views), std::nullopt, stream);

        /// A match is a pair, so the two index vectors describe the same rows of the output and a
        /// difference in their lengths would mean the pairing below is not a pairing at all.
        if (probe_indices->size() != build_indices->size())
            throw std::logic_error(
                "the device returned " + std::to_string(probe_indices->size()) + " probe-side indices against "
                + std::to_string(build_indices->size()) + " build-side ones");

        if (!state.payloads.empty() && !build_indices->is_empty())
        {
            const cudf::column_view build_index_column(
                cudf::data_type{cudf::type_id::INT32},
                static_cast<cudf::size_type>(build_indices->size()),
                build_indices->data(),
                nullptr,
                0);

            /// The gather that keeps the right table on the device: it turns the whole build side
            /// into just the matched rows, so only the join's own output has to travel back.
            ///
            /// `DONT_CHECK` because every index an inner join emits addresses a build row that
            /// matched, by construction - so there is nothing for a bound check to find and
            /// `NULLIFY`, which is the alternative, would only add a pass over the indices and turn
            /// every gathered column nullable. That is cuDF's contract rather than something
            /// checked here; what the copy out does check is that no null appeared anyway, which a
            /// gather of non-nullable columns cannot produce on its own.
            state.gathered_payloads = cudf::gather(
                state.build_payloads_table, build_index_column, cudf::out_of_bounds_policy::DONT_CHECK, stream);
        }

        *num_matches = probe_indices->size();
        state.probe_indices = std::move(probe_indices);
        state.has_result = true;

        /// `key_host_data` is the caller's own column and is released when this returns, so the
        /// transfer above has to be finished reading it by then - the same reason
        /// `clickhouseGPUHashJoinAddBuildBlock` synchronizes.
        stream.synchronize();
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything, for the reason given above.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

int clickhouseGPUHashJoinCopyProbeResultOut(
    void * handle,
    uint32_t * probe_row_indices,
    void * const * payload_host_data,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no handle");

        HashJoinState & state = *static_cast<HashJoinState *>(handle);

        if (!state.has_result)
            throw std::logic_error("a probe's result was copied out before the probe ran");

        const size_t num_matches = state.probe_indices ? state.probe_indices->size() : 0;
        if (num_matches == 0)
            return 0;

        if (probe_row_indices == nullptr)
            throw std::logic_error("nowhere to put " + std::to_string(num_matches) + " probe-side row indices");

        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        /// The indices are copied as the 32-bit values the device produced and read on the host as
        /// unsigned ones, which is the same bits for every non-negative value - and an inner join's
        /// indices are all non-negative. Reinterpreting rather than widening saves the host a pass
        /// over the whole output; the caller checks them against its own row count before indexing.
        static_assert(sizeof(cudf::size_type) == sizeof(uint32_t), "the boundary hands back cuDF's row indices as they are");

        if (const cudaError_t status = cudaMemcpyAsync(
                probe_row_indices,
                state.probe_indices->data(),
                num_matches * sizeof(cudf::size_type),
                cudaMemcpyDeviceToHost,
                stream.value());
            status != cudaSuccess)
            throw std::runtime_error(std::string("cannot copy the probe-side row indices back: ") + cudaGetErrorString(status));

        if (!state.payloads.empty())
        {
            if (!state.gathered_payloads)
                throw std::logic_error("the device kept no gathered payload for a result of " + std::to_string(num_matches) + " rows");

            const cudf::table_view gathered = state.gathered_payloads->view();
            if (static_cast<size_t>(gathered.num_rows()) != num_matches)
                throw std::logic_error(
                    "the device gathered " + std::to_string(gathered.num_rows()) + " payload rows for "
                    + std::to_string(num_matches) + " matches");

            for (size_t i = 0; i < state.payloads.size(); ++i)
            {
                const cudf::column_view column = gathered.column(static_cast<cudf::size_type>(i));

                checkNoNulls(column, "a gathered column of the right table");

                /// `cudf::gather` allocates every output column for itself, so none of them is a
                /// slice of a larger one. `head` would be the wrong pointer if one were, and there
                /// is no `offset` to add to a `void *`.
                if (column.offset() != 0)
                    throw std::logic_error(
                        "the device returned a gathered column of the right table as a slice at offset "
                        + std::to_string(column.offset()));

                if (const cudaError_t status = cudaMemcpyAsync(
                        payload_host_data[i],
                        column.head<void>(),
                        num_matches * state.payloads[i].size,
                        cudaMemcpyDeviceToHost,
                        stream.value());
                    status != cudaSuccess)
                    throw std::runtime_error(
                        std::string("cannot copy a gathered column of the right table back: ") + cudaGetErrorString(status));
            }
        }

        /// The copies were queued on the stream, so the host memory only holds the result once it
        /// has run. Synchronizing once, after all of the columns, rather than once per column.
        stream.synchronize();
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    /// Ok to catch everything, for the reason given above.
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

void clickhouseGPUHashJoinDestroy(void * handle)
{
    /// `delete` frees the build side, the hash table and the last result through the same memory
    /// resource that allocated them, and none of those destructors throws - so there is nothing
    /// here to catch and nothing that could be reported anyway.
    delete static_cast<HashJoinState *>(handle);
}

}
