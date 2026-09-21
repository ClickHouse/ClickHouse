#include <GPU/Utils.h>

#include <cudf/copying.hpp>
#include <cudf/join/hash_join.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>

#include <rmm/device_uvector.hpp>

namespace DB::GPU
{
namespace
{

struct HashJoinState
{
    ElementLayout key;

    std::vector<ElementLayout> payloads;

    rmm::device_buffer build_keys;
    std::vector<rmm::device_buffer> build_payloads;
    size_t build_rows = 0;

    cudf::table_view build_keys_table;
    cudf::table_view build_payloads_table;
    std::unique_ptr<cudf::hash_join> hash_table;
    bool built = false;

    std::unique_ptr<rmm::device_uvector<cudf::size_type>> probe_indices;
    std::unique_ptr<cudf::table> gathered_payloads;
    bool has_result = false;

    const void * external_keys = nullptr;
    std::vector<const void *> external_payloads;
};

}

namespace DB::GPU
{

}
}

namespace DB::GPU
{

int createGPUHashTable(
    GPUElementType key_element_type,
    const GPUElementType * payload_element_types,
    size_t num_payloads,
    GPUHashTableState ** handle,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("nowhere to put the handle");

        *handle = nullptr;

        if (!isIntegerElementType(key_element_type))
            throw std::logic_error("a join key of element type " + std::to_string(static_cast<int>(key_element_type)) + " is not an integer");

        auto state = std::make_unique<HashJoinState>();

        state->key = elementLayoutOf(key_element_type);

        state->payloads.reserve(num_payloads);
        for (size_t i = 0; i < num_payloads; ++i)
            state->payloads.push_back(elementLayoutOf(payload_element_types[i]));

        state->build_payloads.resize(num_payloads);

        setUpDeviceMemoryResourceOnce();

        *handle = reinterpret_cast<GPUHashTableState *>(state.release());
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

int buildGPUHashTable(GPUHashTableState * handle, char * error, size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no handle");

        HashJoinState & state = *reinterpret_cast<HashJoinState *>(handle);

        if (state.built)
            throw std::logic_error("the hash table was built twice");

        state.built = true;

        const auto build_rows = static_cast<cudf::size_type>(state.build_rows);

        const void * key_data = state.external_keys != nullptr ? state.external_keys : state.build_keys.data();

        const std::vector<cudf::column_view> key_views{cudf::column_view(state.key.type, build_rows, key_data, nullptr, 0)};
        state.build_keys_table = cudf::table_view(key_views);

        std::vector<cudf::column_view> payload_views;
        payload_views.reserve(state.payloads.size());
        for (size_t i = 0; i < state.payloads.size(); ++i)
        {
            const void * payload_data
                = state.external_keys != nullptr ? state.external_payloads[i] : state.build_payloads[i].data();
            payload_views.emplace_back(state.payloads[i].type, build_rows, payload_data, nullptr, 0);
        }
        state.build_payloads_table = cudf::table_view(payload_views);

        if (state.build_rows == 0)
            return 0;

        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        state.hash_table = std::make_unique<cudf::hash_join>(state.build_keys_table, cudf::null_equality::EQUAL, stream);
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

int copyGPUMatchesOut(
    GPUHashTableState * handle,
    uint32_t * probe_row_indices,
    void * const * payload_host_data,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no handle");

        HashJoinState & state = *reinterpret_cast<HashJoinState *>(handle);

        if (!state.has_result)
            throw std::logic_error("a probe's result was copied out before the probe ran");

        const size_t num_matches = state.probe_indices ? state.probe_indices->size() : 0;
        if (num_matches == 0)
            return 0;

        if (probe_row_indices == nullptr)
            throw std::logic_error("nowhere to put " + std::to_string(num_matches) + " probe-side row indices");

        const rmm::cuda_stream_view stream = cudf::get_default_stream();

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

void destroyGPUHashTable(GPUHashTableState * handle)
{
    delete reinterpret_cast<HashJoinState *>(handle);
}

int setGPUHashTableBuildSide(
    GPUHashTableState * handle,
    const GPUBuffer * key_buffer,
    const GPUBuffer * const * payload_buffers,
    size_t num_payloads,
    size_t num_rows,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr || key_buffer == nullptr)
            throw std::logic_error("no handle");

        HashJoinState & state = *reinterpret_cast<HashJoinState *>(handle);

        if (state.built)
            throw std::logic_error("the build side was replaced after the hash table was built");
        if (num_payloads != state.payloads.size())
            throw std::logic_error(
                "the build side carries " + std::to_string(num_payloads) + " payload buffers, expected "
                + std::to_string(state.payloads.size()));

        checkRowCountFitsCudf(num_rows, "the right table");

        const auto & keys = *reinterpret_cast<const GPUBufferState *>(key_buffer);
        if (keys.used_bytes != num_rows * state.key.size)
            throw std::logic_error(
                "the key buffer holds " + std::to_string(keys.used_bytes) + " bytes for " + std::to_string(num_rows) + " rows");

        state.external_payloads.resize(num_payloads);
        for (size_t i = 0; i < num_payloads; ++i)
        {
            const auto & payload = *reinterpret_cast<const GPUBufferState *>(payload_buffers[i]);
            if (payload.used_bytes != num_rows * state.payloads[i].size)
                throw std::logic_error(
                    "payload buffer " + std::to_string(i) + " holds " + std::to_string(payload.used_bytes) + " bytes for "
                    + std::to_string(num_rows) + " rows");
            state.external_payloads[i] = payload.values.data();
        }

        state.external_keys = keys.values.data();
        state.build_rows = num_rows;
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

int probeGPUHashTable(
    GPUHashTableState * handle, const GPUBuffer * key_buffer, size_t num_rows, size_t * num_matches, char * error, size_t error_size)
{
    try
    {
        if (handle == nullptr || key_buffer == nullptr)
            throw std::logic_error("no handle");
        if (num_matches == nullptr)
            throw std::logic_error("nowhere to put the number of matches");

        *num_matches = 0;

        HashJoinState & state = *reinterpret_cast<HashJoinState *>(handle);

        if (!state.built)
            throw std::logic_error("a probe before the hash table was built");
        if (num_rows == 0)
            throw std::logic_error("an empty block of the left table");

        checkRowCountFitsCudf(num_rows, "a left block");

        const auto & keys = *reinterpret_cast<const GPUBufferState *>(key_buffer);
        if (keys.used_bytes != num_rows * state.key.size)
            throw std::logic_error(
                "the probe buffer holds " + std::to_string(keys.used_bytes) + " bytes for " + std::to_string(num_rows) + " rows");

        state.gathered_payloads.reset();
        state.probe_indices.reset();
        state.has_result = false;

        if (!state.hash_table)
        {
            state.has_result = true;
            return 0;
        }

        const rmm::cuda_stream_view stream = cudf::get_default_stream();
        const auto probe_rows = static_cast<cudf::size_type>(num_rows);

        const std::vector<cudf::column_view> probe_key_views{
            cudf::column_view(state.key.type, probe_rows, keys.values.data(), nullptr, 0)};

        auto [probe_indices, build_indices]
            = state.hash_table->inner_join(cudf::table_view(probe_key_views), std::nullopt, stream);

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

            state.gathered_payloads
                = cudf::gather(state.build_payloads_table, build_index_column, cudf::out_of_bounds_policy::DONT_CHECK, stream);
        }

        *num_matches = probe_indices->size();
        state.probe_indices = std::move(probe_indices);
        state.has_result = true;

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

}
