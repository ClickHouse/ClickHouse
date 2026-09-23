#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.h>

#include <cstddef>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::GPU
{

struct CompressedBlock
{
    size_t offset = 0;
    size_t compressed_bytes = 0;
    size_t decompressed_bytes = 0;
};

size_t decompressedBytesOf(std::span<const CompressedBlock> blocks);

class Decompressor
{
public:
    struct Piece
    {
        const char * device_compressed = nullptr;
        size_t compressed_bytes = 0;
        std::span<const CompressedBlock> blocks;
        const DeviceEvent * uploaded = nullptr;
    };

    Decompressor();
    ~Decompressor();

    Decompressor(Decompressor && other) noexcept;

    Decompressor(const Decompressor &) = delete;
    Decompressor & operator=(const Decompressor &) = delete;
    Decompressor & operator=(Decompressor &&) = delete;

    void decompress(GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks, char * device_destination);

    void start(GPUCodec codec, std::span<const Piece> pieces);

    const char * finish();

    void release();

private:
    struct Slot
    {
        DeviceBuffer values;
        DeviceEvent copied_out;
        bool in_use = false;
    };

    static constexpr size_t num_slots = 2;

    void queue(GPUCodec codec, std::span<const Piece> pieces, char * destination);

    void waitAndCheck();

    bool in_flight = false;
    char * started_destination = nullptr;

    PinnedBuffer host_arguments;
    PinnedBuffer host_results;

    DeviceBuffer device_compressed;
    DeviceBuffer device_arguments;
    DeviceBuffer device_results;
    DeviceBuffer device_temp;

    DeviceEvent expanded;
    std::vector<size_t> expected_bytes;

    Slot slots[num_slots];
    size_t current_slot = 0;
};

}

#endif
