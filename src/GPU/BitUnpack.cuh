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

/// A block of the `BitPacking` codec on the device, see `BitPackingFormat`, and where its integers go.
struct BitPackingStream
{
    const unsigned char * stream;
    size_t stream_bytes;
    unsigned char * destination;
    size_t unpacked_bytes;
};

/// Unpacks the `num_streams` streams at `streams`, in device memory, of which none has more than `max_chunks` chunks,
/// reading each stream's header on the device. `statuses[i]`, in device memory, is set to 0 where stream `i` is as its
/// header and `unpacked_bytes` say, and to another value where it is not, in which case nothing is said about what was
/// written for it.
__host__ void unpackStreams(const BitPackingStream * streams, size_t num_streams, size_t max_chunks, uint32_t * statuses, cuda::stream_ref stream);

}
