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

/// One column of fixed-width values on the device.
class DeviceColumn
{
public:
    explicit DeviceColumn(GPUElementType element_type_) : element_type(element_type_) { }

    void appendPlain(std::string_view host_values) { values.append(host_values); }

    void appendCompressed(Decompressor & decompressor, GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks);

    /// Makes room for `bytes` more and answers where they go, for the device to write them.
    char * grow(size_t bytes) { return values.grow(bytes); }

    /// Drops the first `num_rows` and moves the rest to the front, through the stream: a kernel
    /// queued over the dropped rows still reads them.
    void dropFront(size_t num_rows);

    void clear() { values.clear(); }

    size_t bytes() const { return values.size(); }

    size_t elementSize() const { return sizeOf(element_type); }

    /// The whole values. Compressed blocks and staging batches cut a column into runs of bytes
    /// rather than of values, so the column may end with the first bytes of a value.
    size_t rows() const { return values.size() / sizeOf(element_type); }

    DeviceColumnView view() const { return {element_type, values.data(), rows()}; }

private:
    const GPUElementType element_type;
    DeviceBuffer values;
    DeviceBuffer spare;
};


/** Fills one device-side column, one block of a ClickHouse column at a time.
  *
  * Values are gathered into pinned host memory and sent in batches of `stage_bytes`, because a copy
  * per block would leave the link idle between blocks. Two staging buffers take turns: while the
  * device reads one, the host fills the other, and an event says when it is free again. That holds
  * across `reset` too - a buffer still being read stays in flight and is waited for only when its
  * turn comes round - so the host stages the next batch while the device works on the last one.
  *
  * A compressed pipe takes compressed blocks instead and has them expanded on the device, so that
  * what crosses the link is the part as it sits on disk. That path does not pipeline - the blocks
  * it sends have to stay put until nvcomp has read them - and synchronizes per batch.
  */
class UploadPipe
{
public:
    static bool canUpload(const IDataType & type);

    UploadPipe(const IDataType & type, size_t stage_bytes_, bool compressed_ = false);

    ~UploadPipe();

    UploadPipe(UploadPipe &&) noexcept = default;

    UploadPipe(const UploadPipe &) = delete;
    UploadPipe & operator=(const UploadPipe &) = delete;
    UploadPipe & operator=(UploadPipe &&) = delete;

    void stage(const IColumn & column);

    /// Room in the staging buffer for up to `max_bytes` of plain values, for the caller to write
    /// straight into and then `commitRaw`; as much as the buffer has, at least one byte.
    std::span<char> reserveRaw(size_t max_bytes);
    void commitRaw(size_t bytes);

    /// Blocks of different codecs are not expanded together: a block of another codec than the
    /// staged ones sends them first.
    void stageCompressedBlock(GPUCodec codec, std::string_view payload, size_t decompressed_bytes);

    size_t stagedRows() const { return staged_bytes / element_size; }

    size_t stagedBytes() const { return staged_bytes; }

    /// Sends what is staged and answers the column, whose rows are all on the device once
    /// `waitForUploads` returns - or at once, for a compressed pipe.
    const DeviceColumn & flush();

    void waitForUploads();

    /// Drops the whole rows and keeps the bytes of a value cut by the last block, for the next
    /// batch to complete. The device column is reused through the stream, so a kernel reading it
    /// need not have run yet; a staging buffer still being copied from is left alone until it is
    /// next needed.
    void reset();

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
    const bool compressed;

    std::optional<GPUCodec> codec;

    Slot slots[num_slots];
    size_t current_slot = 0;

    std::vector<CompressedBlock> blocks;
    Decompressor decompressor;

    size_t staged_bytes = 0;

    DeviceColumn device;
};

}

#endif
