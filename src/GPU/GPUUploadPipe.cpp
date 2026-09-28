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
        cudaMemcpyAsync(spare.grow(tail), values.data() + bytes, tail, cudaMemcpyDeviceToDevice, StreamRegistry::get().compute),
        "Cannot move {} bytes to the front of a device column",
        tail);
    std::swap(values, spare);
}

bool PlainUploadPipe::canUpload(const IDataType & type)
{
    return elementTypeOf(type).has_value();
}

PlainUploadPipe::PlainUploadPipe(const IDataType & type, size_t stage_bytes_)
    : element_type(elementTypeOrThrow(type))
    , element_size(sizeOf(element_type))
    , stage_bytes(stage_bytes_)
    , device(element_type)
{
}

PlainUploadPipe::~PlainUploadPipe()
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

void PlainUploadPipe::stage(const IColumn & column)
{
    const size_t num_rows = column.size();
    if (num_rows == 0)
        return;

    const std::string_view raw = rawValuesOf(column, num_rows, element_size);

    if (currentSlot().staged.size() + raw.size() > stage_bytes)
        sendStagedToDevice();

    if (raw.size() > stage_bytes)
    {
        device.appendPlain(raw);
        staged_bytes += raw.size();
        return;
    }

    currentSlot().staged.append(raw);
    staged_bytes += raw.size();
}

std::span<char> PlainUploadPipe::reserveRaw(size_t max_bytes)
{
    if (currentSlot().staged.size() >= stage_bytes)
        sendStagedToDevice();

    PinnedBuffer & staged = currentSlot().staged;
    staged.reserve(stage_bytes);
    return {staged.data() + staged.size(), std::min(max_bytes, stage_bytes - staged.size())};
}

void PlainUploadPipe::commitRaw(size_t bytes)
{
    currentSlot().staged.grow(bytes);
    staged_bytes += bytes;
}

void PlainUploadPipe::sendStagedToDevice()
{
    Slot & slot = currentSlot();

    if (slot.staged.empty())
        return;

    device.appendPlain(slot.staged.bytes());

    slot.copied.record();
    slot.in_flight = true;

    current_slot = (current_slot + 1) % num_slots;

    Slot & next = currentSlot();
    if (next.in_flight)
    {
        next.copied.wait();
        next.in_flight = false;
    }

    next.staged.clear();
}

const DeviceColumn & PlainUploadPipe::flush()
{
    sendStagedToDevice();
    return device;
}

void PlainUploadPipe::waitForUploads()
{
    for (auto & slot : slots)
    {
        if (!slot.in_flight)
            continue;

        slot.copied.wait();
        slot.in_flight = false;
    }
}

void PlainUploadPipe::reset()
{
    for (auto & slot : slots)
    {
        if (!slot.in_flight)
            slot.staged.clear();
    }

    device.dropFront(device.rows());
    staged_bytes = device.bytes();
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

const DeviceColumn & CompressedUploadPipe::flush()
{
    sendStagedToDevice();
    return device;
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
