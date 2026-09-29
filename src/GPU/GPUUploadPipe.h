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


/// The chars of a column of values of varying width and the offsets its rows end at, starting from 0. `rows` and `view`
/// are of the rows whose chars and sizes have both arrived: all of them when the host appends the two together, and
/// those `settle` counted when they arrive apart, as expanded compressed blocks of either stream do.
class DeviceVariableColumnBuffer final : public IDeviceColumn
{
public:
    explicit DeviceVariableColumnBuffer(rmm::cuda_stream_view stream_);

    void append(std::string_view host_row_ends, std::string_view host_chars);

    char * growChars(size_t bytes) { return chars.grow(bytes); }

    /// Sizes of rows on the device, as bytes: they may start at any address and end in the middle of a size, whose
    /// rest is taken to come with the next call.
    void appendSizes(const char * device_sizes, size_t bytes);

    /// Counts the rows whose chars have arrived. Waits for the stream.
    void settle();

    void dropFront(size_t num_rows);

    void clear();

    /// Bytes of chars and sizes that no settled row covers.
    size_t unsettledBytes() const;

    size_t bytes() const { return chars.size(); }

    size_t rows() const override { return settled_rows; }

    DeviceColumnView view() const override;

private:
    void startOffsets();
    size_t sizedRows() const { return offsets.size() / sizeof(uint64_t) - 1; }

    const rmm::cuda_stream_view stream;
    DeviceBuffer offsets;
    DeviceBuffer chars;
    DeviceBuffer pending_sizes;
    DeviceBuffer spare;
    size_t settled_rows = 0;
    size_t settled_bytes = 0;
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
/// staging area as they are.
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
/// blocking until done. The CPU never decompresses these blocks. A column of values of varying width is two streams,
/// its chars and the sizes of its rows, both staged here as blocks; the device turns the sizes into offsets, and `flush`
/// gives the rows whose chars and sizes have both arrived.
class CompressedUploadPipe final : public IUploadPipe
{
public:
    CompressedUploadPipe(const IDataType & type, size_t stage_bytes_, GPUCodec codec_);

    CompressedUploadPipe(CompressedUploadPipe &&) noexcept = default;

    CompressedUploadPipe(const CompressedUploadPipe &) = delete;
    CompressedUploadPipe & operator=(const CompressedUploadPipe &) = delete;
    CompressedUploadPipe & operator=(CompressedUploadPipe &&) = delete;

    void stageCompressedBlock(std::string_view payload, size_t decompressed_bytes);

    /// Of a column of values of varying width: a block of the sizes of its rows.
    void stageCompressedSizesBlock(std::string_view payload, size_t decompressed_bytes);

    /// Rows whose values, or whose sizes, are staged or on the device since the last `reset`.
    size_t stagedRows() const override;

    size_t stagedBytes() const override { return staged_bytes; }

    const IDeviceColumn & flush() override;

    void reset() override;

private:
    struct StagedBlocks
    {
        PinnedBuffer bytes;
        std::vector<CompressedBlock> blocks;
        size_t expanded_bytes = 0;
    };

    void stageBlock(StagedBlocks & staged, std::string_view payload, size_t decompressed_bytes);
    void sendStagedToDevice();

    const GPUElementType column_type;
    const size_t stage_bytes;
    const GPUCodec codec;

    StagedBlocks staged_data;
    StagedBlocks staged_sizes;
    SyncDecompressor decompressor;
    DeviceBuffer expanded_sizes{StreamRegistry::get().compute};

    /// Expanded bytes of both streams staged or on the device since the last `reset`.
    size_t staged_bytes = 0;

    std::variant<DeviceFixedColumnBuffer, DeviceVariableColumnBuffer> device;
};

}

#endif
