#pragma once

/// Before CCCL, which forward-declares `std::get` for these without libc++'s `abi_tag` - see contrib/cccl-cmake.
#include <array>
#include <tuple>
#include <utility>

#include <cuda/stream_ref>

#include <cstddef>
#include <cstdint>

namespace DB::GPU
{

/// `count` integers of `element_size` bytes, each `reference` plus a value of `bits` bits, packed from the lowest bit of
/// the first byte of `source`, which is readable for 8 bytes past the packed bits.
struct PackedChunk
{
    const unsigned char * source;
    unsigned char * destination;
    uint32_t count;
    uint32_t bits;
    uint32_t element_size;
    uint64_t reference;
};

/// The largest number of bits `unpackChunks` takes, which a read of 8 bytes holds at any bit offset.
constexpr uint32_t max_packed_bits = 56;

/// Writes the integers of the `num_chunks` chunks at `chunks`, in device memory, of which none has more than `max_count`.
__host__ void unpackChunks(const PackedChunk * chunks, size_t num_chunks, size_t max_count, cuda::stream_ref stream);

}
