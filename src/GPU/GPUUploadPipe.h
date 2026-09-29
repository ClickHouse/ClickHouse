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
    explicit DeviceFixedColumnBuffer(GPUElementType element_type_);

    DeviceFixedColumnBuffer(GPUElementType element_type_, rmm::cuda_stream_view stream_);

    void appendPlain(std::string_view host_values) { values.append(host_values); }

    void appendCompressed(SyncDecompressor & decompressor, GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks);

    char * grow(size_t bytes) { return values.grow(bytes); }

    char * mutableData() { return values.data(); }

    void dropFront(size_t num_rows);

    void clear() { values.clear(); }

    size_t bytes() const { return values.size(); }

    size_t elementSize() const { return sizeOf(element_type); }

    size_t rows() const override { return values.size() / sizeOf(element_type); }

    DeviceColumnView view() const override { return fixedView(); }

    DeviceFixedColumn fixedView() const { return {element_type, values.data(), rows()}; }

private:
    const GPUElementType element_type;
    const rmm::cuda_stream_view stream;
    DeviceBuffer values;
    DeviceBuffer spare;
};


class DeviceVariableColumnBuffer final : public IDeviceColumn
{
public:
    explicit DeviceVariableColumnBuffer(rmm::cuda_stream_view stream_);

    void append(std::string_view host_row_ends, std::string_view host_chars);

    void clear();

    size_t bytes() const { return chars.size(); }

    size_t rows() const override { return offsets.size() / sizeof(uint64_t) - 1; }

    DeviceColumnView view() const override;

private:
    void startOffsets();

    const rmm::cuda_stream_view stream;
    DeviceBuffer offsets;
    DeviceBuffer chars;
};


class IUploadPipe
{
public:
    virtual ~IUploadPipe() = default;

    virtual size_t stagedRows() const = 0;

    virtual size_t stagedBytes() const = 0;

    virtual const IDeviceColumn & flush() = 0;

    virtual void reset() = 0;

protected:
    IUploadPipe() = default;
    IUploadPipe(IUploadPipe &&) noexcept = default;
};


/// Uploads plain data, nothing is decompressed here: `stage` takes `IColumn`s that ClickHouse has already
/// decompressed on the CPU while reading, and `reserveRaw`/`commitRaw` take bytes that are written into the
/// staging area as they are. `GroupByGPUAccumulator` uses it only for plain columns; its compressed blocks bypass
/// the pipes and are expanded by its device thread with an `AsyncDecompressor`.
class ColumnUploadPipe final : public IUploadPipe
{
public:
    static bool canUpload(const IDataType & type);

    ColumnUploadPipe(const IDataType & type, size_t stage_bytes_);

    ColumnUploadPipe(const IDataType & type, size_t stage_bytes_, rmm::cuda_stream_view stream_);

    ~ColumnUploadPipe() override;

    ColumnUploadPipe(ColumnUploadPipe &&) noexcept = default;

    ColumnUploadPipe(const ColumnUploadPipe &) = delete;
    ColumnUploadPipe & operator=(const ColumnUploadPipe &) = delete;
    ColumnUploadPipe & operator=(ColumnUploadPipe &&) = delete;

    void stage(const IColumn & column);

    std::span<char> reserveRaw(size_t max_bytes);
    void commitRaw(size_t bytes);

    GPUElementType type() const { return column_type; }

    size_t stagedRows() const override { return staged_rows; }

    size_t stagedBytes() const override { return staged_bytes; }

    const IDeviceColumn & flush() override;

    void reset() override;

    void waitForUploads();

private:
    void stageFixed(const IColumn & column);
    void stageVariable(const IColumn & column);

    void makeRoomFor(size_t bytes);

    void sendStagedToDevice();

    void makeStagingWritable();

    const GPUElementType column_type;
    const size_t stage_bytes;
    const rmm::cuda_stream_view stream;

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


/// Uploads compressed blocks as they are stored in a part and decompresses them on the device: when the staging
/// area fills up and on `flush`, the staged blocks are sent and expanded with nvcomp through a `SyncDecompressor`,
/// blocking until done.
/// The CPU never decompresses these blocks. Used by `GPUAccumulator` for a single aggregation over compressed columns.
class CompressedUploadPipe final : public IUploadPipe
{
public:
    CompressedUploadPipe(const IDataType & type, size_t stage_bytes_, GPUCodec codec_);

    CompressedUploadPipe(CompressedUploadPipe &&) noexcept = default;

    CompressedUploadPipe(const CompressedUploadPipe &) = delete;
    CompressedUploadPipe & operator=(const CompressedUploadPipe &) = delete;
    CompressedUploadPipe & operator=(CompressedUploadPipe &&) = delete;

    void stageCompressedBlock(std::string_view payload, size_t decompressed_bytes);

    size_t stagedRows() const override { return staged_bytes / element_size; }

    size_t stagedBytes() const override { return staged_bytes; }

    const IDeviceColumn & flush() override;

    void reset() override;

private:
    void sendStagedToDevice();

    const GPUElementType element_type;
    const size_t element_size;
    const size_t stage_bytes;
    const GPUCodec codec;

    PinnedBuffer staged;
    std::vector<CompressedBlock> blocks;
    SyncDecompressor decompressor;

    size_t staged_bytes = 0;

    DeviceFixedColumnBuffer device;
};

}

#endif
