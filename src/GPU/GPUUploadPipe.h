#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUDecompression.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.cuh>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <span>
#include <string_view>
#include <variant>
#include <vector>

namespace DB::GPU
{

class IDeviceColumn
{
public:
    virtual ~IDeviceColumn() = default;

    virtual size_t rows() const = 0;

    virtual DeviceColumnView view() const = 0;

protected:
    IDeviceColumn() = default;
    IDeviceColumn(IDeviceColumn &&) noexcept = default;
};


class DeviceFixedColumnBuffer final : public IDeviceColumn
{
public:
    DeviceFixedColumnBuffer(GPUElementType element_type_, cuda::stream_ref stream_);

    void appendPlain(std::string_view host_values) { values.append(host_values); }

    void clear() { values.clear(); }

    size_t bytes() const { return values.size(); }

    size_t rows() const override { return values.size() / sizeOf(element_type); }

    DeviceColumnView view() const override { return DeviceFixedColumn{element_type, values.data(), rows()}; }

private:
    const GPUElementType element_type;
    DeviceBuffer values;
};


/// The chars of a column of values of varying width and the offsets its rows end at, starting from 0.
class DeviceVariableColumnBuffer final : public IDeviceColumn
{
public:
    explicit DeviceVariableColumnBuffer(cuda::stream_ref stream_);

    void append(std::string_view host_row_ends, std::string_view host_chars);

    void clear();

    size_t bytes() const { return chars.size(); }

    size_t rows() const override { return num_rows; }

    DeviceColumnView view() const override;

private:
    void startOffsets();

    const cuda::stream_ref stream;
    DeviceBuffer offsets;
    DeviceBuffer chars;
    size_t num_rows = 0;
};


/// Uploads the columns ClickHouse has read and decompressed on the CPU, through pinned memory.
class ColumnUploadPipe
{
public:
    ColumnUploadPipe(const IDataType & type, size_t stage_bytes_);

    ColumnUploadPipe(const IDataType & type, size_t stage_bytes_, cuda::stream_ref stream_);

    ~ColumnUploadPipe();

    ColumnUploadPipe(ColumnUploadPipe &&) noexcept = default;

    ColumnUploadPipe(const ColumnUploadPipe &) = delete;
    ColumnUploadPipe & operator=(const ColumnUploadPipe &) = delete;
    ColumnUploadPipe & operator=(ColumnUploadPipe &&) = delete;

    void stage(const IColumn & column);

    GPUElementType type() const { return column_type; }

    size_t stagedRows() const { return staged_rows; }

    size_t stagedBytes() const { return staged_bytes; }

    const IDeviceColumn & flush();

    void reset();

    void waitForUploads();

private:
    void stageFixed(const IColumn & column);
    void stageVariable(const IColumn & column);

    void makeRoomFor(size_t bytes);

    void sendStagedToDevice();

    const GPUElementType column_type;
    const size_t stage_bytes;
    const cuda::stream_ref stream;

    PinnedBuffer staged_data;
    PinnedBuffer staged_offsets;
    size_t staging_bytes = 0;
    EventPtr copied = createEvent();
    bool in_flight = false;

    size_t staged_rows = 0;
    size_t staged_bytes = 0;
    size_t staged_chars = 0;

    std::variant<DeviceFixedColumnBuffer, DeviceVariableColumnBuffer> device;
};


}

#endif
