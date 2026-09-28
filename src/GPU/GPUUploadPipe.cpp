#include <GPU/GPUUploadPipe.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUTypeMapping.h>

#include <Common/Exception.h>

#include <algorithm>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

DeviceColumn::DeviceColumn(GPUElementType element_type_)
    : DeviceColumn(element_type_, StreamRegistry::get().compute)
{
}

DeviceColumn::DeviceColumn(GPUElementType element_type_, rmm::cuda_stream_view stream_)
    : element_type(element_type_)
    , stream(stream_)
    , values(stream)
    , spare(stream)
{
}

void DeviceColumn::appendCompressed(
    Decompressor & decompressor, GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks)
{
    decompressor.decompress(codec, host_compressed, blocks, values.grow(decompressedBytesOf(blocks)));
}

void DeviceColumn::dropFront(size_t num_rows)
{
    const size_t bytes = num_rows * sizeOf(element_type);
    if (bytes > values.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Dropping {} rows of a device column of {}", num_rows, rows());

    const size_t tail = values.size() - bytes;
    if (tail == 0)
    {
        values.clear();
        return;
    }

    spare.clear();
    checkCuda(
        cudaMemcpyAsync(spare.grow(tail), values.data() + bytes, tail, cudaMemcpyDeviceToDevice, stream),
        "Cannot move {} bytes to the front of a device column",
        tail);
    std::swap(values, spare);
}

bool ColumnUploadPipe::canUpload(const IDataType & type)
{
    return columnTypeOf(type).has_value();
}

ColumnUploadPipe::ColumnUploadPipe(const IDataType & type, size_t stage_bytes_)
    : ColumnUploadPipe(type, stage_bytes_, StreamRegistry::get().compute)
{
}

ColumnUploadPipe::ColumnUploadPipe(const IDataType & type, size_t stage_bytes_, rmm::cuda_stream_view stream_)
    : layout(ColumnLayout::of(columnTypeOrThrow(type)))
    , stage_bytes(stage_bytes_)
    , stream(stream_)
    , staging(layout.buffers.size())
    , elements_taken(layout.buffers.size())
{
    device.reserve(layout.buffers.size());
    for (size_t i = 0; i < layout.buffers.size(); ++i)
        device.emplace_back(stream);

    startOffsets();
}

ColumnUploadPipe::~ColumnUploadPipe()
{
    try
    {
        waitForUploads();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ColumnUploadPipe::startOffsets()
{
    for (size_t i = 0; i < layout.buffers.size(); ++i)
    {
        if (layout.buffers[i].offsets_into)
            checkCuda(cudaMemsetAsync(device[i].grow(sizeof(UInt64)), 0, sizeof(UInt64), stream), "Cannot start the offsets of a column");
    }
}

void ColumnUploadPipe::makeStagingWritable()
{
    if (!in_flight)
        return;

    copied.wait();
    in_flight = false;

    for (auto & buffer : staging)
        buffer.clear();
    staging_bytes = 0;
}

void ColumnUploadPipe::stage(const IColumn & column)
{
    const size_t num_rows = column.size();
    if (num_rows == 0)
        return;

    const std::vector<std::string_view> block = layout.buffersOf(column);

    size_t block_bytes = 0;
    for (const auto & buffer : block)
        block_bytes += buffer.size();

    if (staging_bytes != 0 && staging_bytes + block_bytes > stage_bytes)
        sendStagedToDevice();

    makeStagingWritable();

    for (size_t i = 0; i < block.size(); ++i)
    {
        const auto & into = layout.buffers[i].offsets_into;
        if (!into)
        {
            staging[i].append(block[i]);
            continue;
        }

        /// The block's offsets are within the block; the column's are within all the blocks.
        const UInt64 moved_by = elements_taken[*into];
        const auto * from = reinterpret_cast<const UInt64 *>(block[i].data());
        auto * to = reinterpret_cast<UInt64 *>(staging[i].grow(block[i].size()));
        for (size_t row = 0; row < block[i].size() / sizeof(UInt64); ++row)
            to[row] = from[row] + moved_by;
    }

    for (size_t i = 0; i < block.size(); ++i)
        elements_taken[i] += block[i].size() / layout.buffers[i].element_size;

    staging_bytes += block_bytes;
    staged_rows += num_rows;
    staged_bytes += block_bytes;
}

std::span<char> ColumnUploadPipe::reserveRaw(size_t max_bytes)
{
    if (layout.type.kind != GPUColumnKind::Fixed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Raw values for a pipe of a column that is not of fixed-width values");

    if (staging_bytes >= stage_bytes)
        sendStagedToDevice();

    makeStagingWritable();

    PinnedBuffer & buffer = staging.front();
    buffer.reserve(stage_bytes);
    return {buffer.data() + buffer.size(), std::min(max_bytes, stage_bytes - buffer.size())};
}

void ColumnUploadPipe::commitRaw(size_t bytes)
{
    staging.front().grow(bytes);
    staging_bytes += bytes;
    elements_taken.front() += bytes / layout.buffers.front().element_size;
    staged_bytes += bytes;
    staged_rows = staged_bytes / layout.buffers.front().element_size;
}

void ColumnUploadPipe::sendStagedToDevice()
{
    /// What is in flight has been sent; what the buffers hold then is only kept until it lands.
    if (staging_bytes == 0 || in_flight)
        return;

    for (size_t i = 0; i < staging.size(); ++i)
        device[i].append(staging[i].bytes());

    copied.record(stream);
    in_flight = true;
}

DeviceColumnView ColumnUploadPipe::flush()
{
    sendStagedToDevice();

    std::vector<char *> data;
    data.reserve(device.size());
    for (auto & buffer : device)
        data.push_back(buffer.data());

    return layout.viewOf(data, staged_rows, elements_taken);
}

void ColumnUploadPipe::waitForUploads()
{
    if (!in_flight)
        return;

    copied.wait();
    in_flight = false;
}

void ColumnUploadPipe::reset()
{
    makeStagingWritable();

    for (auto & buffer : device)
        buffer.clear();
    startOffsets();

    staged_rows = 0;
    staged_bytes = 0;
    std::fill(elements_taken.begin(), elements_taken.end(), 0);
}


CompressedUploadPipe::CompressedUploadPipe(const IDataType & type, size_t stage_bytes_, GPUCodec codec_)
    : element_type(elementTypeOrThrow(type))
    , element_size(sizeOf(element_type))
    , stage_bytes(stage_bytes_)
    , codec(codec_)
    , device(element_type)
{
}

void CompressedUploadPipe::stageCompressedBlock(std::string_view payload, size_t decompressed_bytes)
{
    if (!blocks.empty() && staged.size() + payload.size() > stage_bytes)
        sendStagedToDevice();

    blocks.push_back({
        .offset = staged.size(),
        .compressed_bytes = payload.size(),
        .decompressed_bytes = decompressed_bytes,
    });

    staged.append(payload);
    staged_bytes += decompressed_bytes;
}

void CompressedUploadPipe::sendStagedToDevice()
{
    if (blocks.empty())
        return;

    device.appendCompressed(decompressor, codec, staged.bytes(), blocks);
    blocks.clear();
    staged.clear();
}

DeviceColumnView CompressedUploadPipe::flush()
{
    sendStagedToDevice();
    return device.view();
}

void CompressedUploadPipe::reset()
{
    staged.clear();
    blocks.clear();
    device.dropFront(device.rows());
    staged_bytes = device.bytes();
}

}

#endif
