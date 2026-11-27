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

/// What cuDF takes as the rows of a column.
constexpr size_t max_batch_rows = (1UL << 31) - 1;

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

/// A `String` counts as one byte per row.
size_t rowBytesOf(const std::vector<GPUElementType> & column_types)
{
    size_t bytes = 0;
    for (const GPUElementType type : column_types)
        bytes += isVariable(type) ? 1 : sizeOf(type);
    return bytes;
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
    part_rows.reset();
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

GPUBlockStager::GPUBlockStager(std::vector<GPUElementType> column_types_, size_t batch_bytes, std::shared_ptr<GPUHostBatchPool> pool_)
    : column_types(std::move(column_types_))
    , batch_rows(std::clamp(batch_bytes / std::max<size_t>(rowBytesOf(column_types), 1), size_t{1}, max_batch_rows))
    , pool(std::move(pool_))
{
    if (column_types.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU read of no columns");
    current = pool->take(column_types.size());
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

bool GPUBlockStager::isFull() const
{
    size_t rows = std::numeric_limits<size_t>::max();
    for (size_t c = 0; c < column_types.size(); ++c)
    {
        const GPUHostColumn & column = current->columns[c];
        if (column.values.bytes.size() >= max_column_bytes || column.sizes.bytes.size() >= max_column_bytes)
            return true;
        /// The bytes of a `String` make no rows of their own: its sizes do.
        rows = std::min(rows, isVariable(column_types[c]) ? column.sizes.kept_bytes / sizeof(UInt64) : column.values.kept_bytes / sizeOf(column_types[c]));
    }
    return rows >= batch_rows;
}

void GPUBlockStager::finishBatch(std::optional<size_t> part_rows)
{
    current->part_rows = part_rows;
    finished.push_back(std::move(current));
    current = pool->take(column_types.size());
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
    {
        const CompressedHostColumn & column = streamOrThrow(column_index, stream);
        if (column.isCut() || (!column.empty() && (skipped_bytes != 0 || column.codec != codec)))
            finishBatch();
    }

    CompressedHostColumn & column = streamOrThrow(column_index, stream);
    if (column.blocks.empty())
    {
        column.skipped_bytes = skipped_bytes;
        column.bytes.reserve(std::min(max_column_bytes, std::max(expected_bytes, payload.size())));
    }
    column.kept_bytes += kept_bytes;
    column.codec = codec;

    column.bytes.resize_fill(alignedUp(column.bytes.size()), 0);
    addCompressedBlocksOf(*codec, payload, column.bytes.size(), decompressed_bytes, column.blocks);
    appendHostBytes(column.bytes, payload);

    if (isFull())
        finishBatch();
}

void GPUBlockStager::finishPart(size_t num_rows)
{
    finishBatch(num_rows);
}

}

#endif
