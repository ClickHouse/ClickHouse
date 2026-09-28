#pragma once

#include <GPU/GPUTypes.h>

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

    /// Builds over the right table, which stays where it is, and waits for the device to finish.
    /// Can be called once, and before any probe.
    void build(DeviceColumnView keys, GPUSpan<DeviceColumnView> payloads);

private:
    friend class CudfHashJoinProbe;

    struct State;
    State * state = nullptr;
};

/** One probe of a `CudfHashJoin` at a time, on a non-blocking stream of its own: the keys of a
  * left block go up, the matches stay on the device until `copyMatchesOut`, and nothing waits for
  * any other probe. A probe is used by one thread at a time; the join must outlive it.
  */
class CudfHashJoinProbe
{
public:
    explicit CudfHashJoinProbe(const CudfHashJoin & join);
    ~CudfHashJoinProbe();

    CudfHashJoinProbe(const CudfHashJoinProbe &) = delete;
    CudfHashJoinProbe & operator=(const CudfHashJoinProbe &) = delete;

    /// Sends `num_rows` keys of the join's key type from `host_keys`, which must be pinned, probes
    /// with them, keeps the matches on the device, and answers how many there are.
    size_t probe(const char * host_keys, size_t num_rows);

    /// Copies the last probe's matches into pinned host memory and waits for them: the probe-side
    /// row index of each matching pair, as `UInt32`, and the right table's payload columns gathered
    /// to the same order.
    void copyMatchesOut(HostColumnView probe_row_indices, GPUSpan<HostColumnView> payloads);

private:
    struct State;
    State * state = nullptr;
};

}
