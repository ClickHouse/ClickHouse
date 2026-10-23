#pragma once

#include <GPU/GPUTypes.cuh>

/// Before CCCL, which forward-declares `std::get` for these without libc++'s `abi_tag` - see contrib/cccl-cmake.
#include <array>
#include <tuple>
#include <utility>

#include <cuda/stream_ref>

#include <cstddef>
#include <cstdint>

namespace DB::GPU
{

/// The pairs of rows one probe of a `CudfHashJoin` matched, as row indices on the device.
class CudfJoinMatches
{
public:
    CudfJoinMatches() = default;
    __host__ CudfJoinMatches(CudfJoinMatches && other) noexcept;
    __host__ CudfJoinMatches & operator=(CudfJoinMatches && other) noexcept;
    __host__ ~CudfJoinMatches();

    CudfJoinMatches(const CudfJoinMatches &) = delete;
    CudfJoinMatches & operator=(const CudfJoinMatches &) = delete;

    __host__ size_t size() const;

    /// Copies the row indices of the pairs, on the left and on the right, to the host, and waits for them on `stream`.
    __host__ void copyOut(uint32_t * probe_indices, uint32_t * build_indices, cuda::stream_ref stream) const;

    /// The row indices of the pairs on the device, on the left and on the right, there once the probe's stream has them.
    __host__ const int32_t * probeIndices() const;
    __host__ const int32_t * buildIndices() const;

private:
    friend class CudfHashJoin;

    struct State;
    State * state = nullptr;
};

/** A hash table of cuDF over the keys of the right table of an `INNER JOIN`.
  *
  * Only keys are on the device: the build keys, which the hash table views and which must outlive it, and the keys of
  * a probe. What a probe returns is, for each matching pair, the index of its row on either side, from which the host
  * gathers the columns of both sides as they are, of any type. Once built, the table is only read, so blocks of the
  * left table are probed at once, each on a stream of its own.
  */
class CudfHashJoin
{
public:
    /// Builds the table on the compute stream, and waits for it.
    __host__ CudfHashJoin(GPUElementType key_type, DeviceFixedColumn build_keys);
    __host__ ~CudfHashJoin();

    CudfHashJoin(const CudfHashJoin &) = delete;
    CudfHashJoin & operator=(const CudfHashJoin &) = delete;

    /// Matches the keys of a block of the left table on `stream`.
    __host__ CudfJoinMatches probe(DeviceFixedColumn probe_keys, cuda::stream_ref stream) const;

private:
    struct State;
    State * state = nullptr;
};

}
