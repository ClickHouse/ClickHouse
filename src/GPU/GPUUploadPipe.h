#pragma once

#include "config.h"

#if USE_GPU

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
    explicit DeviceColumn(GPUElementType element_type_) : element_type(element_type_) { }

    void appendPlain(std::string_view host_values) { values.append(host_values); }

    void appendCompressed(Decompressor & decompressor, GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks);

    char * grow(size_t bytes) { return values.grow(bytes); }

    void dropFront(size_t num_rows);

    void clear() { values.clear(); }

    size_t bytes() const { return values.size(); }

    size_t elementSize() const { return sizeOf(element_type); }

    size_t rows() const { return values.size() / sizeOf(element_type); }

    DeviceColumnView view() const { return {element_type, values.data(), rows()}; }

private:
    const GPUElementType element_type;
    DeviceBuffer values;
    DeviceBuffer spare;
};


/** Gathers the values of one column on the host and sends them to the device, where they make up
  * one `DeviceColumn`. What an implementation takes, and how it sends it, is its own; this is what
  * the user of the column on the device sees.
  */
class IUploadPipe
{
public:
    virtual ~IUploadPipe() = default;

    /// The rows taken since the last `reset`, sent or not.
    virtual size_t stagedRows() const = 0;

    virtual size_t stagedBytes() const = 0;

    /// Sends what is staged and returns the column on the device.
    virtual const DeviceColumn & flush() = 0;

    /// Drops the rows on the device, keeping its memory for the next batch.
    virtual void reset() = 0;

protected:
    IUploadPipe() = default;
    IUploadPipe(IUploadPipe &&) noexcept = default;
};


/** Takes the values as they are - the column of a block, or bytes written straight into its
  * staging buffer - and copies them to the device. Two staging slots in pinned memory take turns:
  * while the copy of one runs, the other is filled.
  */
class PlainUploadPipe final : public IUploadPipe
{
public:
    static bool canUpload(const IDataType & type);

    PlainUploadPipe(const IDataType & type, size_t stage_bytes_);

    ~PlainUploadPipe() override;

    PlainUploadPipe(PlainUploadPipe &&) noexcept = default;

    PlainUploadPipe(const PlainUploadPipe &) = delete;
    PlainUploadPipe & operator=(const PlainUploadPipe &) = delete;
    PlainUploadPipe & operator=(PlainUploadPipe &&) = delete;

    void stage(const IColumn & column);

    std::span<char> reserveRaw(size_t max_bytes);
    void commitRaw(size_t bytes);

    size_t stagedRows() const override { return staged_bytes / element_size; }

    size_t stagedBytes() const override { return staged_bytes; }

    const DeviceColumn & flush() override;

    void reset() override;

    void waitForUploads();

private:
    struct Slot
    {
        PinnedBuffer staged;
        DeviceEvent copied;
        bool in_flight = false;

        Slot() = default;

        Slot(Slot && other) noexcept
            : staged(std::move(other.staged)), copied(std::move(other.copied)), in_flight(std::exchange(other.in_flight, false))
        {
        }
    };

    static constexpr size_t num_slots = 2;

    void sendStagedToDevice();

    Slot & currentSlot() { return slots[current_slot]; }

    const GPUElementType element_type;
    const size_t element_size;
    const size_t stage_bytes;

    Slot slots[num_slots];
    size_t current_slot = 0;

    size_t staged_bytes = 0;

    DeviceColumn device;
};


/** Takes the compressed blocks of a column file, all of one codec, and has the device expand
  * them. Sending is synchronous - it returns once the blocks are expanded - so one staging buffer
  * is enough.
  */
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

    const DeviceColumn & flush() override;

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
