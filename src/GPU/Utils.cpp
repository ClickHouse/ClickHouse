#include <GPU/Utils.h>

#include <nvcomp/lz4.h>
#include <nvcomp/zstd.h>

namespace DB::GPU
{

void decompressBlocksIntoDevice(
    GPUCodec codec,
    const void * host_data,
    const size_t * compressed_offsets,
    const size_t * compressed_bytes,
    const size_t * decompressed_bytes,
    size_t num_blocks,
    size_t compressed_total,
    size_t decompressed_total,
    size_t max_decompressed,
    void * destination,
    rmm::cuda_stream_view stream)
{
    const rmm::device_buffer device_compressed(host_data, compressed_total, stream);

    std::vector<const void *> host_compressed_ptrs(num_blocks);
    std::vector<void *> host_value_ptrs(num_blocks);
    for (size_t i = 0, at = 0; i < num_blocks; ++i)
    {
        host_compressed_ptrs[i] = static_cast<const char *>(device_compressed.data()) + compressed_offsets[i];
        host_value_ptrs[i] = static_cast<char *>(destination) + at;
        at += decompressed_bytes[i];
    }

    const rmm::device_buffer d_compressed_ptrs(host_compressed_ptrs.data(), num_blocks * sizeof(void *), stream);
    const rmm::device_buffer d_value_ptrs(host_value_ptrs.data(), num_blocks * sizeof(void *), stream);
    const rmm::device_buffer d_compressed_bytes(compressed_bytes, num_blocks * sizeof(size_t), stream);
    const rmm::device_buffer d_decompressed_bytes(decompressed_bytes, num_blocks * sizeof(size_t), stream);
    rmm::device_buffer d_actual_bytes(num_blocks * sizeof(size_t), stream);
    rmm::device_buffer d_statuses(num_blocks * sizeof(nvcompStatus_t), stream);

    size_t temp_bytes = 0;
    nvcompStatus_t status = codec == GPUCodec::ZSTD
        ? nvcompBatchedZstdDecompressGetTempSizeAsync(
              num_blocks, max_decompressed, nvcompBatchedZstdDecompressDefaultOpts, &temp_bytes, decompressed_total)
        : nvcompBatchedLZ4DecompressGetTempSizeAsync(
              num_blocks, max_decompressed, nvcompBatchedLZ4DecompressDefaultOpts, &temp_bytes, decompressed_total);
    if (status != nvcompSuccess)
        throw std::logic_error("nvcomp could not size its scratch space: " + std::to_string(static_cast<int>(status)));

    rmm::device_buffer device_temp(temp_bytes, stream);

    status = codec == GPUCodec::ZSTD
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
              stream.value());
    if (status != nvcompSuccess)
        throw std::logic_error("nvcomp could not decompress: " + std::to_string(static_cast<int>(status)));

    std::vector<size_t> actual_bytes(num_blocks);
    std::vector<nvcompStatus_t> statuses(num_blocks);

    const auto copy_back = [&stream](void * dst, const void * src, size_t bytes, const char * what)
    {
        if (const cudaError_t copy_status = cudaMemcpyAsync(dst, src, bytes, cudaMemcpyDeviceToHost, stream.value());
            copy_status != cudaSuccess)
            throw std::runtime_error(std::string("cannot copy ") + what + " back: " + cudaGetErrorString(copy_status));
    };

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
}

}
