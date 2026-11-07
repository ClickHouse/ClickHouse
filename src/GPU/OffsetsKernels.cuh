#pragma once

#include <rmm/cuda_stream_view.hpp>

#include <cstddef>
#include <cstdint>

namespace DB::GPU
{

/// Turns the sizes of `count` strings into the offsets their ends are at: `offsets_end[0]` is where the first of them
/// starts, and `offsets_end[1]` to `offsets_end[count]` are written.
/// Moves `count` offsets back by `minus`; `from` and `to` may be the same.
__host__ void subtractFromOffsets(const uint64_t * from, size_t count, uint64_t minus, uint64_t * to, rmm::cuda_stream_view stream);

__host__ void offsetsFromSizes(const uint64_t * sizes, size_t count, uint64_t * offsets_end, rmm::cuda_stream_view stream);

struct CoveredRows
{
    size_t rows = 0;
    uint64_t bytes = 0;
};

/// How many of the rows that `offsets` (`num_rows + 1` of them, from 0) delimit end within `chars_bytes`, and where
/// the last of them ends. Waits for the stream.
__host__ CoveredRows rowsCoveredBy(const uint64_t * offsets, size_t num_rows, uint64_t chars_bytes, rmm::cuda_stream_view stream);

}
