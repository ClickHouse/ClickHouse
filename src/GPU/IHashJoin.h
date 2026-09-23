#pragma once

#include <GPU/GPUTypes.h>

namespace DB::GPU
{

class IHashJoin
{
public:
    virtual ~IHashJoin() = default;

    /// Builds over the right table, which stays where it is: the hash table views the key column
    /// and the payload columns, so they have to outlive it. Can be called once.
    virtual void build(DeviceColumnView keys, GPUSpan<DeviceColumnView> payloads) = 0;

    /// Probes with one block of the left table, keeps the matches on the device for
    /// `copyMatchesOut`, and answers how many there are.
    virtual size_t probe(DeviceColumnView keys) = 0;

    /// Copies the last probe's matches back: the probe-side row index of each matching pair, as
    /// `UInt32`, and the right table's payload columns gathered to the same order.
    virtual void copyMatchesOut(HostColumnView probe_row_indices, GPUSpan<HostColumnView> payloads) = 0;

    static IHashJoin * create(GPUElementType key_element_type, GPUSpan<GPUElementType> payload_element_types);
};

}
