#pragma once

#include <GPU/IHashJoin.h>

#include <cudf/join/hash_join.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>

#include <rmm/device_uvector.hpp>

#include <memory>
#include <vector>

namespace DB::GPU
{

/// A hash table over the right table, and the matches of the last probe.
///
/// Nothing here owns the right table's values: `build_payloads` views the buffers the host side
/// filled, and `cudf::hash_join` views the key buffer. They belong to the `GPU::HashTable` that
/// built this one, which outlives it.
class CudfHashJoin final : public IHashJoin
{
public:
    CudfHashJoin(GPUElementType key_element_type_, GPUSpan<GPUElementType> payload_element_types_);

    void build(DeviceColumnView keys, GPUSpan<DeviceColumnView> payloads) override;

    size_t probe(DeviceColumnView keys) override;

    void copyMatchesOut(HostColumnView probe_row_indices, GPUSpan<HostColumnView> payloads) override;

private:
    const GPUElementType key_element_type;
    const std::vector<GPUElementType> payload_element_types;

    cudf::table_view build_payloads;
    std::unique_ptr<cudf::hash_join> hash_join;
    bool built = false;

    std::unique_ptr<rmm::device_uvector<cudf::size_type>> probe_indices;
    std::unique_ptr<cudf::table> gathered_payloads;
    bool probed = false;
};

}
