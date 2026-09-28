#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDecompression.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::GPU
{

class DeviceColumn
{
public:
    /// On the compute stream.
    explicit DeviceColumn(GPUElementType element_type_);

    /// Allocated, copied into and freed in the order of `stream_`.
    DeviceColumn(GPUElementType element_type_, rmm::cuda_stream_view stream_);

    void appendPlain(std::string_view host_values) { values.append(host_values); }

    void appendCompressed(Decompressor & decompressor, GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks);

    char * grow(size_t bytes) { return values.grow(bytes); }

    char * mutableData() { return values.data(); }

    void dropFront(size_t num_rows);

    void clear() { values.clear(); }

    size_t bytes() const { return values.size(); }

    size_t elementSize() const { return sizeOf(element_type); }

    size_t rows() const { return values.size() / sizeOf(element_type); }

    DeviceColumnView view() const { return {element_type, values.data(), rows()}; }

private:
    const GPUElementType element_type;
    const rmm::cuda_stream_view stream;
    DeviceBuffer values;
    DeviceBuffer spare;
};


class IUploadPipe
{
public:
    virtual ~IUploadPipe() = default;

    virtual size_t stagedRows() const = 0;

    virtual size_t stagedBytes() const = 0;

    virtual DeviceColumnView flush() = 0;

    virtual void reset() = 0;

protected:
    IUploadPipe() = default;
    IUploadPipe(IUploadPipe &&) noexcept = default;
};


class ColumnUploadPipe final : public IUploadPipe
{
public:
    static bool canUpload(const IDataType & type);

    /// Copies on the compute stream.
    ColumnUploadPipe(const IDataType & type, size_t stage_bytes_);

    /// Copies on `stream_`, which has to outlive the pipe.
    ColumnUploadPipe(const IDataType & type, size_t stage_bytes_, rmm::cuda_stream_view stream_);

    ~ColumnUploadPipe() override;

    ColumnUploadPipe(ColumnUploadPipe &&) noexcept = default;

    ColumnUploadPipe(const ColumnUploadPipe &) = delete;
    ColumnUploadPipe & operator=(const ColumnUploadPipe &) = delete;
    ColumnUploadPipe & operator=(ColumnUploadPipe &&) = delete;

    void stage(const IColumn & column);

    std::span<char> reserveRaw(size_t max_bytes);
    void commitRaw(size_t bytes);

    GPUColumnType type() const { return layout.type; }

    size_t stagedRows() const override { return staged_rows; }

    size_t stagedBytes() const override { return staged_bytes; }

    DeviceColumnView flush() override;

    void reset() override;

    void waitForUploads();

private:
    void sendStagedToDevice();

    void makeStagingWritable();

    void startOffsets();

    const ColumnLayout layout;
    const size_t stage_bytes;
    const rmm::cuda_stream_view stream;

    std::vector<PinnedBuffer> staging;
    size_t staging_bytes = 0;
    DeviceEvent copied;
    bool in_flight = false;

    size_t staged_rows = 0;
    size_t staged_bytes = 0;
    std::vector<size_t> elements_taken;

    std::vector<DeviceBuffer> device;
};


class CompressedUploadPipe final : public IUploadPipe
{
public:
    CompressedUploadPipe(const IDataType & type, size_t stage_bytes_, GPUCodec codec_);

    CompressedUploadPipe(CompressedUploadPipe &&) noexcept = default;

    CompressedUploadPipe(const CompressedUploadPipe &) = delete;
    CompressedUploadPipe & operator=(const CompressedUploadPipe &) = delete;
    CompressedUploadPipe & operator=(CompressedUploadPipe &&) = delete;

    void stageCompressedBlock(std::string_view payload, size_t decompressed_bytes);

    /// Counts the values the staged blocks expand to.
    size_t stagedRows() const override { return staged_bytes / element_size; }

    size_t stagedBytes() const override { return staged_bytes; }

    DeviceColumnView flush() override;

    void reset() override;

private:
    void sendStagedToDevice();

    const GPUElementType element_type;
    const size_t element_size;
    const size_t stage_bytes;
    const GPUCodec codec;

    PinnedBuffer staged;
    std::vector<CompressedBlock> blocks;
    Decompressor decompressor;

    size_t staged_bytes = 0;

    DeviceColumn device;
};

}

#endif
