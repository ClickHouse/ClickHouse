#include <GPU/Utils.h>

namespace DB::GPU
{

int createGPUBuffer(GPUElementType element_type, GPUBuffer ** handle, char * error, size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("nowhere to put the handle");

        *handle = nullptr;

        setUpDeviceMemoryResourceOnce();

        auto state = std::make_unique<GPUBufferState>();
        state->element = elementLayoutOf(element_type);

        *handle = reinterpret_cast<GPUBuffer *>(state.release());
        return 0;
    }
    catch (const std::exception & e)
    {
        return handleException(error, error_size, e.what());
    }
    catch (...)
    {
        return handleException(error, error_size, "unknown exception");
    }
}

void destroyGPUBuffer(GPUBuffer * handle)
{
    delete reinterpret_cast<GPUBufferState *>(handle);
}

void clearGPUBuffer(GPUBuffer * handle)
{
    if (handle != nullptr)
        reinterpret_cast<GPUBufferState *>(handle)->used_bytes = 0;
}

int appendToGPUBuffer(GPUBuffer * handle, const void * host_data, size_t bytes, char * error, size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no buffer to append to");

        if (bytes == 0)
            return 0;

        auto & state = *reinterpret_cast<GPUBufferState *>(handle);
        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        reserveColumnBuffer(state.values, state.used_bytes + bytes, stream);

        if (const cudaError_t status = cudaMemcpyAsync(
                static_cast<char *>(state.values.data()) + state.used_bytes,
                host_data,
                bytes,
                cudaMemcpyHostToDevice,
                stream.value());
            status != cudaSuccess)
            throw std::runtime_error(std::string("cannot copy values to the device: ") + cudaGetErrorString(status));

        state.used_bytes += bytes;
        return 0;
    }
    catch (const std::exception & e)
    {
        return handleException(error, error_size, e.what());
    }
    catch (...)
    {
        return handleException(error, error_size, "unknown exception");
    }
}

int appendCompressedToGPUBuffer(
    GPUBuffer * handle,
    GPUCodec codec,
    const void * host_data,
    const size_t * compressed_offsets,
    const size_t * compressed_bytes,
    const size_t * decompressed_bytes,
    size_t num_blocks,
    char * error,
    size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no buffer to append to");

        if (num_blocks == 0)
            return 0;

        auto & state = *reinterpret_cast<GPUBufferState *>(handle);
        const rmm::cuda_stream_view stream = cudf::get_default_stream();

        const CompressedTotals totals = compressedTotalsOf(compressed_offsets, compressed_bytes, decompressed_bytes, num_blocks);

        reserveColumnBuffer(state.values, state.used_bytes + totals.decompressed, stream);

        decompressBlocksIntoDevice(
            codec,
            host_data,
            compressed_offsets,
            compressed_bytes,
            decompressed_bytes,
            num_blocks,
            totals.compressed,
            totals.decompressed,
            totals.max_decompressed,
            static_cast<char *>(state.values.data()) + state.used_bytes,
            stream);

        state.used_bytes += totals.decompressed;
        return 0;
    }
    catch (const std::exception & e)
    {
        return handleException(error, error_size, e.what());
    }
    catch (...)
    {
        return handleException(error, error_size, "unknown exception");
    }
}

int syncGPUBuffer(GPUBuffer * handle, char * error, size_t error_size)
{
    try
    {
        if (handle == nullptr)
            throw std::logic_error("no buffer to synchronize");

        cudf::get_default_stream().synchronize();
        return 0;
    }
    catch (const std::exception & e)
    {
        return handleException(error, error_size, e.what());
    }
    catch (...)
    {
        return handleException(error, error_size, "unknown exception");
    }
}

int gpuBufferRows(GPUBuffer * handle, size_t * num_rows, char * error, size_t error_size)
{
    try
    {
        if (handle == nullptr || num_rows == nullptr)
            throw std::logic_error("no buffer to measure");

        const auto & state = *reinterpret_cast<const GPUBufferState *>(handle);
        if (state.used_bytes % state.element.size != 0)
            throw std::logic_error(
                "the buffer holds " + std::to_string(state.used_bytes) + " bytes, not a whole number of "
                + std::to_string(state.element.size) + "-byte values");

        *num_rows = state.used_bytes / state.element.size;
        return 0;
    }
    catch (const std::exception & e)
    {
        return handleException(error, error_size, e.what());
    }
    catch (...)
    {
        return handleException(error, error_size, "unknown exception");
    }
}

struct GPUMarkerState
{
    cudaEvent_t event = nullptr;
};

GPUMarker * createGPUMarker(char * error, size_t error_size)
{
    try
    {
        setUpDeviceMemoryResourceOnce();

        auto state = std::make_unique<GPUMarkerState>();
        if (const cudaError_t status = cudaEventCreateWithFlags(&state->event, cudaEventDisableTiming);
            status != cudaSuccess)
            throw std::runtime_error(std::string("cannot create a CUDA event: ") + cudaGetErrorString(status));

        return reinterpret_cast<GPUMarker *>(state.release());
    }
    catch (const std::exception & e)
    {
        writeError(error, error_size, e.what());
        return nullptr;
    }
    catch (...)
    {
        writeError(error, error_size, "unknown exception");
        return nullptr;
    }
}

void destroyGPUMarker(GPUMarker * marker)
{
    if (marker == nullptr)
        return;

    auto * state = reinterpret_cast<GPUMarkerState *>(marker);
    if (state->event != nullptr)
        cudaEventDestroy(state->event);
    delete state;
}

int recordGPUMarker(GPUMarker * marker, char * error, size_t error_size)
{
    try
    {
        if (marker == nullptr)
            throw std::logic_error("no marker to record");

        auto & state = *reinterpret_cast<GPUMarkerState *>(marker);
        if (const cudaError_t status = cudaEventRecord(state.event, cudf::get_default_stream().value());
            status != cudaSuccess)
            throw std::runtime_error(std::string("cannot record a CUDA event: ") + cudaGetErrorString(status));

        return 0;
    }
    catch (const std::exception & e)
    {
        return handleException(error, error_size, e.what());
    }
    catch (...)
    {
        return handleException(error, error_size, "unknown exception");
    }
}

int waitGPUMarker(GPUMarker * marker, char * error, size_t error_size)
{
    try
    {
        if (marker == nullptr)
            throw std::logic_error("no marker to wait for");

        auto & state = *reinterpret_cast<GPUMarkerState *>(marker);
        if (const cudaError_t status = cudaEventSynchronize(state.event); status != cudaSuccess)
            throw std::runtime_error(std::string("cannot wait for a CUDA event: ") + cudaGetErrorString(status));

        return 0;
    }
    catch (const std::exception & e)
    {
        return handleException(error, error_size, e.what());
    }
    catch (...)
    {
        return handleException(error, error_size, "unknown exception");
    }
}

}
