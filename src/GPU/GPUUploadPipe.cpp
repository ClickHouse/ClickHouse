#include <GPU/GPUUploadPipe.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

DeviceFixedColumnBuffer::DeviceFixedColumnBuffer(GPUElementType element_type_, cuda::stream_ref stream_)
    : element_type(element_type_), values(stream_)
{
}

DeviceVariableColumnBuffer::DeviceVariableColumnBuffer(cuda::stream_ref stream_)
    : stream(stream_), offsets(stream), chars(stream)
{
    startOffsets();
}

void DeviceVariableColumnBuffer::startOffsets()
{
    checkCuda(cudaMemsetAsync(offsets.grow(sizeof(uint64_t)), 0, sizeof(uint64_t), stream.get()), "Cannot start the offsets of a column");
}

void DeviceVariableColumnBuffer::append(std::string_view host_row_ends, std::string_view host_chars)
{
    if (host_row_ends.size() % sizeof(uint64_t) != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of the offsets of a column of values of varying width", host_row_ends.size());

    offsets.append(host_row_ends);
    chars.append(host_chars);
    num_rows += host_row_ends.size() / sizeof(uint64_t);
}

void DeviceVariableColumnBuffer::clear()
{
    offsets.clear();
    chars.clear();
    num_rows = 0;
    startOffsets();
}

DeviceColumnView DeviceVariableColumnBuffer::view() const
{
    return DeviceVariableColumn{
        .offsets = reinterpret_cast<const uint64_t *>(offsets.data()),
        .chars = chars.data(),
        .rows = num_rows,
        .chars_bytes = chars.size(),
    };
}

namespace
{

std::variant<DeviceFixedColumnBuffer, DeviceVariableColumnBuffer> deviceColumnFor(GPUElementType type, cuda::stream_ref stream)
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

ColumnUploadPipe::ColumnUploadPipe(const IDataType & type, size_t stage_bytes_)
    : ColumnUploadPipe(type, stage_bytes_, GPUResources::get().compute)
{
}

ColumnUploadPipe::ColumnUploadPipe(const IDataType & type, size_t stage_bytes_, cuda::stream_ref stream_)
    : column_type(uploadTypeOrThrow(type))
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

void ColumnUploadPipe::makeRoomFor(size_t bytes)
{
    if (staging_bytes != 0 && staging_bytes + bytes > stage_bytes)
        sendStagedToDevice();

    waitForUploads();
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
    appendHostBytes(staged_data, values);

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

    auto * to = reinterpret_cast<UInt64 *>(growBy(staged_offsets, offsets.size() * sizeof(UInt64)));
    for (size_t row = 0; row < offsets.size(); ++row)
        to[row] = offsets[row] + staged_chars;
    appendHostBytes(staged_data, {reinterpret_cast<const char *>(chars.data()), chars.size()});

    staged_chars += chars.size();
    staging_bytes += block_bytes;
    staged_bytes += block_bytes;
}

void ColumnUploadPipe::sendStagedToDevice()
{
    if (staging_bytes == 0 || in_flight)
        return;

    /// Until `in_flight` is set, nothing waits for the copies from the staging buffers, which the pool may give away.
    waitOnFailure(stream, [&]
    {
        switch (columnKindOf(column_type))
        {
            case GPUColumnKind::Fixed:
                std::get<DeviceFixedColumnBuffer>(device).appendPlain(bytesOf(staged_data));
                break;
            case GPUColumnKind::Variable:
                std::get<DeviceVariableColumnBuffer>(device).append(bytesOf(staged_offsets), bytesOf(staged_data));
                break;
        }

        checkCuda(cudaEventRecord(copied.get(), stream.get()), "Cannot mark a point in an upload stream");
    });
    in_flight = true;
}

const IDeviceColumn & ColumnUploadPipe::flush()
{
    sendStagedToDevice();
    waitForUploads();
    return std::visit([](const auto & column) -> const IDeviceColumn & { return column; }, device);
}

void ColumnUploadPipe::waitForUploads()
{
    if (!in_flight)
        return;

    checkCuda(cudaEventSynchronize(copied.get()), "Cannot wait for an upload to the device");
    in_flight = false;

    staged_data.clear();
    staged_offsets.clear();
    staging_bytes = 0;
}

void ColumnUploadPipe::reset()
{
    waitForUploads();

    std::visit([](auto & column) { column.clear(); }, device);

    /// `waitForUploads` clears the staging only after an upload, but a batch below the threshold may still be staged.
    staged_data.clear();
    staged_offsets.clear();
    staging_bytes = 0;
    staged_rows = 0;
    staged_bytes = 0;
    staged_chars = 0;
}

}

#endif
