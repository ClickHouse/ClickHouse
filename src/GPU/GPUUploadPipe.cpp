#include <GPU/GPUUploadPipe.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

DeviceFixedColumnBuffer::DeviceFixedColumnBuffer(GPUElementType element_type_)
    : DeviceFixedColumnBuffer(element_type_, StreamRegistry::get().compute)
{
}

DeviceFixedColumnBuffer::DeviceFixedColumnBuffer(GPUElementType element_type_, rmm::cuda_stream_view stream_)
    : element_type(element_type_)
    , stream(stream_)
    , values(stream)
    , spare(stream)
{
}

void DeviceFixedColumnBuffer::appendCompressed(
    SyncDecompressor & decompressor, GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks)
{
    decompressor.decompress(codec, host_compressed, blocks, values.grow(decompressedBytesOf(blocks)));
}

void DeviceFixedColumnBuffer::dropFront(size_t num_rows)
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

DeviceVariableColumnBuffer::DeviceVariableColumnBuffer(rmm::cuda_stream_view stream_)
    : stream(stream_)
    , offsets(stream)
    , chars(stream)
{
    startOffsets();
}

void DeviceVariableColumnBuffer::startOffsets()
{
    checkCuda(cudaMemsetAsync(offsets.grow(sizeof(uint64_t)), 0, sizeof(uint64_t), stream), "Cannot start the offsets of a column");
}

void DeviceVariableColumnBuffer::append(std::string_view host_row_ends, std::string_view host_chars)
{
    if (host_row_ends.size() % sizeof(uint64_t) != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of the offsets of a column of values of varying width", host_row_ends.size());

    offsets.append(host_row_ends);
    chars.append(host_chars);
}

void DeviceVariableColumnBuffer::clear()
{
    offsets.clear();
    chars.clear();
    startOffsets();
}

DeviceColumnView DeviceVariableColumnBuffer::view() const
{
    return DeviceVariableColumn{
        .offsets = reinterpret_cast<const uint64_t *>(offsets.data()),
        .chars = chars.data(),
        .rows = rows(),
        .chars_bytes = chars.size(),
    };
}

namespace
{

std::variant<DeviceFixedColumnBuffer, DeviceVariableColumnBuffer> deviceColumnFor(GPUElementType type, rmm::cuda_stream_view stream)
{
    switch (columnKindOf(type))
    {
        case GPUColumnKind::Fixed:
            return std::variant<DeviceFixedColumnBuffer, DeviceVariableColumnBuffer>(std::in_place_index<0>, type, stream);
        case GPUColumnKind::Variable:
            return std::variant<DeviceFixedColumnBuffer, DeviceVariableColumnBuffer>(std::in_place_index<1>, stream);
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU column kind of element type {}", type);
}

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
    : column_type(columnTypeOrThrow(type))
    , stage_bytes(stage_bytes_)
    , stream(stream_)
    , device(deviceColumnFor(column_type, stream))
{
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

void ColumnUploadPipe::makeStagingWritable()
{
    if (!in_flight)
        return;

    checkCuda(cudaEventSynchronize(copied.get()), "Cannot wait for an upload to the device");
    in_flight = false;

    staged_data.clear();
    staged_offsets.clear();
    staging_bytes = 0;
}

void ColumnUploadPipe::makeRoomFor(size_t bytes)
{
    if (staging_bytes != 0 && staging_bytes + bytes > stage_bytes)
        sendStagedToDevice();

    makeStagingWritable();
}

void ColumnUploadPipe::stage(const IColumn & column)
{
    const size_t num_rows = column.size();
    if (num_rows == 0)
        return;

    switch (columnKindOf(column_type))
    {
        case GPUColumnKind::Fixed:
            stageFixed(column);
            break;
        case GPUColumnKind::Variable:
            stageVariable(column);
            break;
    }

    staged_rows += num_rows;
}

void ColumnUploadPipe::stageFixed(const IColumn & column)
{
    const std::string_view values = rawValuesOf(column, column.size(), sizeOf(column_type));

    makeRoomFor(values.size());
    staged_data.append(values);

    staging_bytes += values.size();
    staged_bytes += values.size();
}

void ColumnUploadPipe::stageVariable(const IColumn & column)
{
    const auto * strings = typeid_cast<const ColumnString *>(&column);
    if (!strings)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of {} where the device takes strings", column.getName());

    const auto & offsets = strings->getOffsets();
    const auto & chars = strings->getChars();
    const size_t block_bytes = offsets.size() * sizeof(UInt64) + chars.size();

    makeRoomFor(block_bytes);

    auto * to = reinterpret_cast<UInt64 *>(staged_offsets.grow(offsets.size() * sizeof(UInt64)));
    for (size_t row = 0; row < offsets.size(); ++row)
        to[row] = offsets[row] + staged_chars;
    staged_data.append({reinterpret_cast<const char *>(chars.data()), chars.size()});

    staged_chars += chars.size();
    staging_bytes += block_bytes;
    staged_bytes += block_bytes;
}

std::span<char> ColumnUploadPipe::reserveRaw(size_t max_bytes)
{
    if (columnKindOf(column_type) != GPUColumnKind::Fixed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Raw values for a pipe of a column that is not of fixed-width values");

    if (staging_bytes >= stage_bytes)
        sendStagedToDevice();

    makeStagingWritable();

    staged_data.reserve(stage_bytes);
    return {staged_data.data() + staged_data.size(), std::min(max_bytes, stage_bytes - staged_data.size())};
}

void ColumnUploadPipe::commitRaw(size_t bytes)
{
    staged_data.grow(bytes);
    staging_bytes += bytes;
    staged_bytes += bytes;
    staged_rows = staged_bytes / sizeOf(column_type);
}

void ColumnUploadPipe::sendStagedToDevice()
{
    if (staging_bytes == 0 || in_flight)
        return;

    switch (columnKindOf(column_type))
    {
        case GPUColumnKind::Fixed:
            std::get<DeviceFixedColumnBuffer>(device).appendPlain(staged_data.bytes());
            break;
        case GPUColumnKind::Variable:
            std::get<DeviceVariableColumnBuffer>(device).append(staged_offsets.bytes(), staged_data.bytes());
            break;
    }

    checkCuda(cudaEventRecord(copied.get(), stream), "Cannot mark a point in an upload stream");
    in_flight = true;
}

const IDeviceColumn & ColumnUploadPipe::flush()
{
    sendStagedToDevice();
    return std::visit([](const auto & column) -> const IDeviceColumn & { return column; }, device);
}

void ColumnUploadPipe::waitForUploads()
{
    if (!in_flight)
        return;

    checkCuda(cudaEventSynchronize(copied.get()), "Cannot wait for an upload to the device");
    in_flight = false;
}

void ColumnUploadPipe::reset()
{
    makeStagingWritable();

    std::visit([](auto & column) { column.clear(); }, device);

    staged_rows = 0;
    staged_bytes = 0;
    staged_chars = 0;
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

const IDeviceColumn & CompressedUploadPipe::flush()
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
