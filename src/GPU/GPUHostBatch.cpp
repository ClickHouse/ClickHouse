#include <GPU/GPUHostBatch.h>

#if USE_GPU

#include <GPU/GPUTypeMapping.h>

#include <Common/Exception.h>

#include <algorithm>
#include <limits>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::GPU
{

namespace
{

/// Where each compressed block starts on the device, as nvcomp reads them, see `nvcompZstdRequiredDecompressionAlignment`.
constexpr size_t compressed_alignment = 8;

size_t alignedUp(size_t bytes)
{
    return (bytes + compressed_alignment - 1) / compressed_alignment * compressed_alignment;
}

bool isVariable(GPUElementType type)
{
    return columnKindOf(type) == GPUColumnKind::Variable;
}

}

void GPUHostColumn::clear()
{
    values.clear();
    sizes.clear();
}

void GPUHostBatch::clear()
{
    for (auto & column : columns)
        column.clear();
    rows = 0;
}

std::unique_ptr<GPUHostBatch> GPUHostBatchPool::take(size_t num_columns)
{
    std::unique_ptr<GPUHostBatch> batch;
    {
        std::lock_guard lock(mutex);
        if (!free_batches.empty())
        {
            batch = std::move(free_batches.back());
            free_batches.pop_back();
        }
    }
    if (!batch)
        batch = std::make_unique<GPUHostBatch>();
    batch->clear();
    batch->columns.resize(num_columns);
    return batch;
}

void GPUHostBatchPool::give(std::unique_ptr<GPUHostBatch> batch)
{
    std::lock_guard lock(mutex);
    free_batches.push_back(std::move(batch));
}

GPUBlockStager::GPUBlockStager(std::vector<GPUElementType> column_types_, std::shared_ptr<GPUHostBatchPool> pool_)
    : column_types(std::move(column_types_))
    , pool(std::move(pool_))
{
    if (column_types.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU read of no columns");
    startBatch();
}

void GPUBlockStager::startBatch()
{
    current = pool->take(column_types.size());
    for (size_t c = 0; c < column_types.size(); ++c)
        current->columns[c].type = column_types[c];
}

CompressedHostColumn & GPUBlockStager::streamOrThrow(size_t column, GPUStream stream)
{
    if (column >= column_types.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU read of {} columns", column, column_types.size());
    if (stream == GPUStream::Values)
        return current->columns[column].values;
    if (!isVariable(column_types[column]))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of column {} of a GPU read, which is of a fixed width", column);
    return current->columns[column].sizes;
}

void GPUBlockStager::finishBatch(size_t rows)
{
    current->rows = rows;
    finished.push_back(std::move(current));
    startBatch();
}

std::unique_ptr<GPUHostBatch> GPUBlockStager::takeFinished()
{
    if (finished.empty())
        return nullptr;
    auto batch = std::move(finished.front());
    finished.pop_front();
    return batch;
}

void GPUBlockStager::addCompressedBlock(
    size_t column_index,
    GPUStream stream,
    UInt8 method,
    std::string_view payload,
    size_t decompressed_bytes,
    size_t expected_bytes,
    size_t skipped_bytes,
    size_t kept_bytes)
{
    const auto codec = codecOf(method);
    /// The plan knows the codecs of the table, not those of the blocks of a part: a part written before an `ALTER` of a
    /// codec, or by the adaptive codec, may hold blocks of others.
    if (!codec)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A block of compression method {:#x} read for the GPU, which the device does not expand; such a part is aggregated "
            "on the CPU with `allow_experimental_gpu_aggregation = 0`",
            static_cast<UInt16>(method));

    if (skipped_bytes + kept_bytes > decompressed_bytes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "{} bytes kept after {} skipped of a block of {} for the GPU", kept_bytes, skipped_bytes, decompressed_bytes);

    /// A batch has one run of bytes of each stream: it starts at a block that skips bytes, and ends at one cut short.
    CompressedHostColumn & column = streamOrThrow(column_index, stream);
    if (column.isCut() || (!column.empty() && skipped_bytes != 0))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A block for the GPU that skips {} bytes after the first of a range, or after one cut short, of column {}",
            skipped_bytes,
            column_index);

    if (column.empty())
    {
        column.skipped_bytes = skipped_bytes;
        column.bytes.reserve(std::max(expected_bytes, payload.size()));
    }
    column.kept_bytes += kept_bytes;

    column.bytes.resize_fill(alignedUp(column.bytes.size()), 0);
    addCompressedBlocksOf(*codec, payload, column.bytes.size(), decompressed_bytes, column.blocks);
    appendHostBytes(column.bytes, payload);
}

}

#endif
