#include <GPU/GPUColumns.h>

#include <GPU/GPUCall.h>

#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUTypes.h>

#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

namespace
{

constexpr size_t error_buffer_size = 1024;

}

void GPUMarkerDeleter::operator()(GPUMarker * marker) const noexcept
{
    destroyGPUMarker(marker);
}

void GPUBufferDeleter::operator()(GPUBuffer * buffer) const noexcept
{
    destroyGPUBuffer(buffer);
}

ColumnBuffer::ColumnBuffer(GPUElementType element_type_)
    : element_type(element_type_)
{
    GPUBuffer * created = nullptr;
    char error[error_buffer_size] = {};

    if (createGPUBuffer(element_type, &created, error, sizeof(error)) != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot allocate a device buffer: {}", error);

    buffer_on_gpu.reset(created);
}

ColumnBuffer::~ColumnBuffer() = default;
ColumnBuffer::ColumnBuffer(ColumnBuffer &&) noexcept = default;
ColumnBuffer & ColumnBuffer::operator=(ColumnBuffer &&) noexcept = default;

void ColumnBuffer::appendPlain(const char * host_data, size_t bytes)
{
    char error[error_buffer_size] = {};

    if (appendToGPUBuffer(buffer_on_gpu.get(), host_data, bytes, error, sizeof(error)) != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot send {} bytes to the device: {}", bytes, error);
}

void ColumnBuffer::appendCompressed(
    GPUCodec codec,
    const char * host_data,
    const std::vector<size_t> & compressed_offsets,
    const std::vector<size_t> & compressed_bytes,
    const std::vector<size_t> & decompressed_bytes)
{
    if (compressed_offsets.size() != compressed_bytes.size() || compressed_bytes.size() != decompressed_bytes.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The compressed block descriptors disagree on how many blocks there are");

    char error[error_buffer_size] = {};

    if (appendCompressedToGPUBuffer(
            buffer_on_gpu.get(),
            codec,
            host_data,
            compressed_offsets.data(),
            compressed_bytes.data(),
            decompressed_bytes.data(),
            compressed_offsets.size(),
            error,
            sizeof(error))
        != 0)
        throw Exception(
            ErrorCodes::GPU_ERROR, "Cannot decode {} compressed blocks on the device: {}", compressed_offsets.size(), error);
}

void ColumnBuffer::sync()
{
    char error[error_buffer_size] = {};

    if (syncGPUBuffer(buffer_on_gpu.get(), error, sizeof(error)) != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot wait for the device to finish an upload: {}", error);
}

void ColumnBuffer::clear()
{
    clearGPUBuffer(buffer_on_gpu.get());
}

size_t ColumnBuffer::rows() const
{
    char error[error_buffer_size] = {};
    size_t num_rows = 0;

    if (gpuBufferRows(buffer_on_gpu.get(), &num_rows, error, sizeof(error)) != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot measure a device buffer: {}", error);

    return num_rows;
}

bool UploadPipe::canUpload(const IDataType & type)
{
    return elementTypeOf(type).has_value();
}

UploadPipe::UploadPipe(const IDataType & type, size_t stage_bytes_, std::optional<GPUCodec> codec_)
    : element_type(elementTypeOf(type).value())
    , element_size(sizeOf(element_type))
    , stage_bytes(stage_bytes_)
    , codec(codec_)
    , device(element_type)
{
    for (auto & slot : slots)
    {
        slot.staged.reserve(stage_bytes);

        char error[1024] = {};
        slot.copied.reset(createGPUMarker(error, sizeof(error)));
        if (!slot.copied)
            throw Exception(ErrorCodes::GPU_ERROR, "Cannot create a GPU marker for the upload pipe: {}", error);
    }
}

void UploadPipe::stage(const IColumn & column)
{
    if (codec)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A pipe that decodes compressed blocks cannot also take plain values");

    const size_t num_rows = column.size();
    if (num_rows == 0)
        return;

    const std::string_view raw = rawValuesOf(column, num_rows, element_size);

    if (currentSlot().staged.size() + raw.size() > stage_bytes)
        sendStagedToDevice();

    if (raw.size() > stage_bytes)
    {
        device.appendPlain(raw.data(), raw.size());
        device.sync();
        staged_rows += num_rows;
        return;
    }

    currentSlot().staged.append(raw.data(), raw.size());
    staged_rows += num_rows;
}

void UploadPipe::stageCompressedBlock(const char * payload, size_t compressed_bytes, size_t decompressed_bytes)
{
    if (!codec)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A pipe without a codec cannot take compressed blocks");

    if (currentSlot().staged.size() + compressed_bytes > stage_bytes && !block_offsets.empty())
        sendStagedToDevice();

    block_offsets.push_back(currentSlot().staged.size());
    block_compressed_sizes.push_back(compressed_bytes);
    block_decompressed_sizes.push_back(decompressed_bytes);

    currentSlot().staged.append(payload, compressed_bytes);
    staged_rows += decompressed_bytes / element_size;
}

void UploadPipe::sendStagedToDevice()
{
    Slot & slot = currentSlot();

    if (slot.staged.empty())
        return;

    if (codec)
    {
        device.appendCompressed(*codec, slot.staged.data(), block_offsets, block_compressed_sizes, block_decompressed_sizes);

        block_offsets.clear();
        block_compressed_sizes.clear();
        block_decompressed_sizes.clear();

        device.sync();
        slot.staged.clear();
        return;
    }

    device.appendPlain(slot.staged.data(), slot.staged.size());

    call([&](char * e, size_t n) { return recordGPUMarker(slot.copied.get(), e, n); },
         "Cannot mark the end of an upload to a GPU");
    slot.in_flight = true;

    current_slot = (current_slot + 1) % num_slots;

    Slot & next = currentSlot();
    if (next.in_flight)
    {
        call([&](char * e, size_t n) { return waitGPUMarker(next.copied.get(), e, n); },
             "Cannot wait for an upload to a GPU to finish");
        next.in_flight = false;
    }

    next.staged.clear();
}

ColumnBuffer & UploadPipe::flush()
{
    sendStagedToDevice();
    return device;
}

void UploadPipe::waitForUploads()
{
    for (auto & slot : slots)
    {
        if (!slot.in_flight)
            continue;

        call([&](char * e, size_t n) { return waitGPUMarker(slot.copied.get(), e, n); },
             "Cannot wait for an upload to a GPU to finish");
        slot.in_flight = false;
    }
}

void UploadPipe::reset()
{
    waitForUploads();

    for (auto & slot : slots)
        slot.staged.clear();

    current_slot = 0;
    block_offsets.clear();
    block_compressed_sizes.clear();
    block_decompressed_sizes.clear();
    staged_rows = 0;
    device.clear();
}

}

#endif
