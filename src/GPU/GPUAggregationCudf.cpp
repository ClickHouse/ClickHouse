#include <GPU/GPUAggregationCudf.h>

#include <cudf/aggregation.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/groupby.hpp>
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

#include <nvcomp/lz4.h>
#include <nvcomp/zstd.h>

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

void writeSum(const cudf::scalar & sum, int sum_type, void * result, rmm::cuda_stream_view stream)
{
    if (!sum.is_valid(stream))
        throw std::runtime_error("the device returned no sum for a non-empty batch of values without nulls");

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


void reduceIntoSum(const cudf::column_view & column, int sum_type, void * result, rmm::cuda_stream_view stream)
{
    const auto aggregation = cudf::make_sum_aggregation<cudf::reduce_aggregation>();
    const auto sum = cudf::reduce(column, *aggregation, sumTypeOf(sum_type), stream);

    writeSum(*sum, sum_type, result, stream);
}

constexpr size_t sum_type_size = 8;

void reduceDeviceValues(
    const void * device_values, int element_type, int sum_type, size_t num_rows, void * result, rmm::cuda_stream_view stream)
{
    const ElementLayout element = elementLayoutOf(element_type);
    const cudf::column_view column(
        element.type, static_cast<cudf::size_type>(num_rows), device_values, nullptr, 0);
    reduceIntoSum(column, sum_type, result, stream);
}

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

void checkNoNulls(const cudf::column_view & column, const std::string & what)
{
    if (column.null_count() != 0)
        throw std::logic_error(
            "the device returned " + std::to_string(column.null_count()) + " nulls in " + what
            + ", where the input had no null mask at all");
}

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

struct GroupBySumState
{
    std::vector<ElementLayout> keys;

    std::vector<ElementLayout> values;
    std::vector<cudf::data_type> sum_types;
    std::unique_ptr<cudf::table> partial;

    bool finalized = false;
};

void checkRowCountFitsCudf(size_t num_rows, const std::string & what)
{
    if (num_rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
        throw std::logic_error(what + " of " + std::to_string(num_rows) + " rows is too large for cuDF");
}

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

        if (num_rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
            throw std::logic_error("a batch of " + std::to_string(num_rows) + " rows is too large for cuDF");

        const ElementLayout element = elementLayoutOf(element_type);

        setUpDeviceMemoryResourceOnce();

        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        const rmm::device_buffer device_data(host_data, num_rows * element.size, stream);

        reduceDeviceValues(device_data.data(), element_type, sum_type, num_rows, result, stream);
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

int clickhouseGPUSumCompressed(
    int codec,
    int element_type,
    int sum_type,
    const void * host_data,
    const size_t * compressed_offsets,
    const size_t * compressed_bytes,
    const size_t * decompressed_bytes,
    size_t num_blocks,
    size_t num_rows,
    void * result,
    char * error,
    size_t error_size)
{
    try
    {
        if (num_blocks == 0 || num_rows == 0)
            throw std::logic_error("nothing to sum");

        checkRowCountFitsCudf(num_rows, "a batch");

        const ElementLayout element = elementLayoutOf(element_type);

        size_t compressed_total = 0;
        size_t decompressed_total = 0;
        size_t max_decompressed = 0;
        for (size_t i = 0; i < num_blocks; ++i)
        {
            compressed_total = std::max(compressed_total, compressed_offsets[i] + compressed_bytes[i]);
            decompressed_total += decompressed_bytes[i];
            max_decompressed = std::max(max_decompressed, decompressed_bytes[i]);
        }

        if (decompressed_total != num_rows * element.size)
            throw std::logic_error(
                "blocks expand to " + std::to_string(decompressed_total) + " bytes, expected "
                + std::to_string(num_rows * element.size));

        setUpDeviceMemoryResourceOnce();

        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        const rmm::device_buffer device_compressed(host_data, compressed_total, stream);
        rmm::device_buffer device_values(decompressed_total, stream);

        std::vector<const void *> host_compressed_ptrs(num_blocks);
        std::vector<void *> host_value_ptrs(num_blocks);
        for (size_t i = 0, at = 0; i < num_blocks; ++i)
        {
            host_compressed_ptrs[i] = static_cast<const char *>(device_compressed.data()) + compressed_offsets[i];
            host_value_ptrs[i] = static_cast<char *>(device_values.data()) + at;
            at += decompressed_bytes[i];
        }

        const rmm::device_buffer d_compressed_ptrs(host_compressed_ptrs.data(), num_blocks * sizeof(void *), stream);
        const rmm::device_buffer d_value_ptrs(host_value_ptrs.data(), num_blocks * sizeof(void *), stream);
        const rmm::device_buffer d_compressed_bytes(compressed_bytes, num_blocks * sizeof(size_t), stream);
        const rmm::device_buffer d_decompressed_bytes(decompressed_bytes, num_blocks * sizeof(size_t), stream);
        rmm::device_buffer d_actual_bytes(num_blocks * sizeof(size_t), stream);
        rmm::device_buffer d_statuses(num_blocks * sizeof(nvcompStatus_t), stream);

        size_t temp_bytes = 0;
        nvcompStatus_t status = codec == CLICKHOUSE_GPU_CODEC_ZSTD
            ? nvcompBatchedZstdDecompressGetTempSizeAsync(
                  num_blocks, max_decompressed, nvcompBatchedZstdDecompressDefaultOpts, &temp_bytes, decompressed_total)
            : nvcompBatchedLZ4DecompressGetTempSizeAsync(
                  num_blocks, max_decompressed, nvcompBatchedLZ4DecompressDefaultOpts, &temp_bytes, decompressed_total);
        if (status != nvcompSuccess)
            throw std::logic_error("nvcomp could not size its scratch space: " + std::to_string(static_cast<int>(status)));

        rmm::device_buffer device_temp(temp_bytes, stream);

        status = codec == CLICKHOUSE_GPU_CODEC_ZSTD
            ? nvcompBatchedZstdDecompressAsync(
                  static_cast<const void * const *>(d_compressed_ptrs.data()),
                  static_cast<const size_t *>(d_compressed_bytes.data()),
                  static_cast<const size_t *>(d_decompressed_bytes.data()),
                  static_cast<size_t *>(d_actual_bytes.data()),
                  num_blocks,
                  device_temp.data(),
                  temp_bytes,
                  static_cast<void * const *>(d_value_ptrs.data()),
                  nvcompBatchedZstdDecompressDefaultOpts,
                  static_cast<nvcompStatus_t *>(d_statuses.data()),
                  stream.value())
            : nvcompBatchedLZ4DecompressAsync(
                  static_cast<const void * const *>(d_compressed_ptrs.data()),
                  static_cast<const size_t *>(d_compressed_bytes.data()),
                  static_cast<const size_t *>(d_decompressed_bytes.data()),
                  static_cast<size_t *>(d_actual_bytes.data()),
                  num_blocks,
                  device_temp.data(),
                  temp_bytes,
                  static_cast<void * const *>(d_value_ptrs.data()),
                  nvcompBatchedLZ4DecompressDefaultOpts,
                  static_cast<nvcompStatus_t *>(d_statuses.data()),
                  stream.value())
            ;
        if (status != nvcompSuccess)
            throw std::logic_error("nvcomp could not decompress: " + std::to_string(static_cast<int>(status)));

        const auto copy_back = [&stream](void * destination, const void * source, size_t bytes, const char * what)
        {
            if (const cudaError_t copy_status
                = cudaMemcpyAsync(destination, source, bytes, cudaMemcpyDeviceToHost, stream.value());
                copy_status != cudaSuccess)
                throw std::runtime_error(std::string("cannot copy ") + what + " back: " + cudaGetErrorString(copy_status));
        };

        std::vector<size_t> actual_bytes(num_blocks);
        std::vector<nvcompStatus_t> statuses(num_blocks);
        copy_back(actual_bytes.data(), d_actual_bytes.data(), num_blocks * sizeof(size_t), "the decompressed sizes");
        copy_back(statuses.data(), d_statuses.data(), num_blocks * sizeof(nvcompStatus_t), "the decompression statuses");
        stream.synchronize();

        for (size_t i = 0; i < num_blocks; ++i)
        {
            if (statuses[i] != nvcompSuccess)
                throw std::logic_error(
                    "block " + std::to_string(i) + " did not decompress: " + std::to_string(static_cast<int>(statuses[i])));
            if (actual_bytes[i] != decompressed_bytes[i])
                throw std::logic_error(
                    "block " + std::to_string(i) + " expanded to " + std::to_string(actual_bytes[i]) + " bytes, expected "
                    + std::to_string(decompressed_bytes[i]));
        }

        reduceDeviceValues(device_values.data(), element_type, sum_type, num_rows, result, stream);
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
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

        setUpDeviceMemoryResourceOnce();

        *handle = state.release();
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
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

        if (num_rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
            throw std::logic_error("a batch of " + std::to_string(num_rows) + " rows is too large for cuDF");

        const auto batch_rows = static_cast<cudf::size_type>(num_rows);
        const rmm::cuda_stream_view stream = cudf::get_default_stream();

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

        *num_groups = state.partial ? static_cast<size_t>(state.partial->num_rows()) : 0;
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
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

        stream.synchronize();
        return 0;
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return 1;
    }
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return 1;
    }
}

void clickhouseGPUGroupBySumDestroy(void * handle)
{
    delete static_cast<GroupBySumState *>(handle);
}

int clickhouseGPUAllocPinned(size_t bytes, void ** host_ptr, char * error, size_t error_size)
{
    *host_ptr = nullptr;

    if (bytes == 0)
        return 0;

    const cudaError_t status = cudaHostAlloc(host_ptr, bytes, cudaHostAllocDefault);
    if (status != cudaSuccess)
    {
        *host_ptr = nullptr;
        writeError(error, error_size, cudaGetErrorString(status));
        return 1;
    }

    return 0;
}

void clickhouseGPUFreePinned(void * host_ptr)
{
    if (host_ptr != nullptr)
        cudaFreeHost(host_ptr);
}

}
