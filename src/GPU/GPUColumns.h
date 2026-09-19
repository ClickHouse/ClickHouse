#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB::GPU
{

class PinnedBuffer
{
public:
    PinnedBuffer() = default;
    explicit PinnedBuffer(size_t capacity_) { reserve(capacity_); }
    ~PinnedBuffer();

    PinnedBuffer(PinnedBuffer && other) noexcept;
    PinnedBuffer & operator=(PinnedBuffer && other) noexcept;

    PinnedBuffer(const PinnedBuffer &) = delete;
    PinnedBuffer & operator=(const PinnedBuffer &) = delete;

    void reserve(size_t bytes);

    void append(const char * data, size_t bytes);

    void clear() { used = 0; }

    const char * data() const { return buffer; }
    size_t size() const { return used; }
    bool empty() const { return used == 0; }

private:
    char * buffer = nullptr;
    size_t capacity = 0;
    size_t used = 0;
};


class ColumnBuffer
{
public:
    explicit ColumnBuffer(GPUElementType element_type);
    ~ColumnBuffer();

    ColumnBuffer(ColumnBuffer &&) noexcept;
    ColumnBuffer & operator=(ColumnBuffer &&) noexcept;

    ColumnBuffer(const ColumnBuffer &) = delete;
    ColumnBuffer & operator=(const ColumnBuffer &) = delete;

    void appendPlain(const char * host_data, size_t bytes);

    void appendCompressed(
        GPUCodec codec,
        const char * host_data,
        const std::vector<size_t> & compressed_offsets,
        const std::vector<size_t> & compressed_bytes,
        const std::vector<size_t> & decompressed_bytes);

    void sync();

    void clear();

    size_t rows() const;

    const GPUBuffer * buffer() const { return buffer_on_gpu.get(); }

private:
    GPUElementType element_type;
    GPUBufferPtr buffer_on_gpu;
};

class UploadPipe
{
public:
    static bool canUpload(const IDataType & type);

    UploadPipe(const IDataType & type, size_t stage_bytes, std::optional<GPUCodec> codec = {});

    void stage(const IColumn & column);

    void stageCompressedBlock(const char * payload, size_t compressed_bytes, size_t decompressed_bytes);

    size_t stagedRows() const { return staged_rows; }

    ColumnBuffer & flush();

    void waitForUploads();

    void reset();

private:
    struct Slot
    {
        PinnedBuffer staged;
        GPUMarkerPtr copied;
        bool in_flight = false;
    };

    static constexpr size_t num_slots = 2;

    void sendStagedToDevice();

    Slot & currentSlot() { return slots[current_slot]; }

    const GPUElementType element_type;
    const size_t element_size;
    const size_t stage_bytes;
    const std::optional<GPUCodec> codec;

    Slot slots[num_slots];
    size_t current_slot = 0;

    std::vector<size_t> block_offsets;
    std::vector<size_t> block_compressed_sizes;
    std::vector<size_t> block_decompressed_sizes;

    size_t staged_rows = 0;

    ColumnBuffer device;
};

}

#endif
