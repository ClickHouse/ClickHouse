#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/** A `cudf::hash_join` over the right table of an `INNER JOIN`.
  *
  * Nothing here owns the right table's values: the hash table views the key column and the
  * payload columns the host side uploaded, so they have to outlive it. They belong to the
  * `GPU::HashTable` that built this one, which does.
  *
  * Once built it is only read, and any number of `CudfHashJoinProbe` probe it at once, each on a
  * stream of its own.
  *
  * This header is what the host sees of the classes; the state behind the pointers is the nvcc
  * island's, which alone includes cuDF.
  */
class CudfHashJoin
{
public:
    CudfHashJoin(GPUElementType key_element_type, GPUSpan<GPUElementType> payload_element_types);
    ~CudfHashJoin();

    CudfHashJoin(const CudfHashJoin &) = delete;
    CudfHashJoin & operator=(const CudfHashJoin &) = delete;

    /// Queues the build over the right table, which stays where it is, on the compute stream. Can
    /// be called once, and before any probe; the probes run on streams of their own, so the caller
    /// waits for the compute stream before the first.
    void build(DeviceFixedColumn keys, GPUSpan<DeviceFixedColumn> payloads);

private:
    friend class CudfHashJoinProbe;

    struct State;
    State * state = nullptr;
};

/** One probe of a `CudfHashJoin` at a time, on a stream the caller owns: the keys of a left block
  * are already on the device, and the matches stay there, in memory of the probe's, until the next
  * probe. A probe is used by one thread at a time; the join and the stream must outlive it.
  */
class CudfHashJoinProbe
{
public:
    CudfHashJoinProbe(const CudfHashJoin & join, rmm::cuda_stream_view stream);
    ~CudfHashJoinProbe();

    CudfHashJoinProbe(const CudfHashJoinProbe &) = delete;
    CudfHashJoinProbe & operator=(const CudfHashJoinProbe &) = delete;

    /// Probes with `keys`, whose upload the caller has queued on the probe's stream, and answers
    /// how many pairs match. The count comes back to the host, so this waits for the device.
    size_t probe(DeviceFixedColumn keys);

    /// The last probe's matches: the probe-side row index of each matching pair, as `UInt32`, and
    /// the right table's payload columns gathered to the same order. Valid until the next probe.
    DeviceFixedColumn probeRowIndices() const;
    DeviceFixedColumn gatheredPayload(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

}
