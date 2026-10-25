#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cstdint>
#include <functional>
#include <memory>

namespace DB::GPU
{

/** A set of integer keys on the device, built once from a column there, that says of a column of keys which of them it
  * holds: the keys of the right table of a hash join, by which the device filters the rows of the left table as it reads
  * them, as a runtime filter does on the CPU. Built with a hash set of cuCollections, over the keys widened to 64 bits.
  */
class DeviceKeySet
{
public:
    /// Builds the set on `stream` from the values of `keys`, and waits for it.
    DeviceKeySet(DeviceFixedColumn keys, cuda::stream_ref stream);
    ~DeviceKeySet();

    DeviceKeySet(const DeviceKeySet &) = delete;
    DeviceKeySet & operator=(const DeviceKeySet &) = delete;

    GPUElementType keyType() const;

    /// Queues on `stream`: `out[i]` is 1 where value `i` of `needles`, of `keyType`, is in the set, and 0 otherwise.
    void contains(DeviceFixedColumn needles, uint8_t * out, cuda::stream_ref stream) const;

private:
    struct State;
    State * state = nullptr;
};

/// Whoever comes to have a set of keys for a runtime filter: a hash join, once its right table is read.
class IDeviceKeySetProvider
{
public:
    virtual ~IDeviceKeySetProvider() = default;

    virtual GPUElementType keyElementType() const = 0;

    /// Waits until the set is there, and returns it, or nothing once `stop` says to stop waiting.
    virtual std::shared_ptr<const DeviceKeySet> waitForKeySet(const std::function<bool()> & stop) const = 0;
};

}

#endif
