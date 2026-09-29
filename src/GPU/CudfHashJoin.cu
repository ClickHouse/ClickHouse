#include <GPU/CudfHashJoin.cuh>

#include <GPU/Cudf.cuh>

#include <cudf/copying.hpp>
#include <cudf/join/hash_join.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>

#include <rmm/device_uvector.hpp>

#include <memory>
#include <string>
#include <vector>

namespace DB::GPU
{

namespace
{

GPUElementType integerKeyTypeOf(GPUElementType key_element_type)
{
    if (!isInteger(key_element_type))
        throwGPUError("a join key of element type " + std::to_string(static_cast<int>(key_element_type)) + " is not an integer");

    return key_element_type;
}

void checkCount(size_t actual, size_t expected, const std::string & what)
{
    if (actual != expected)
        throwGPUError(std::to_string(actual) + " " + what + ", expected " + std::to_string(expected));
}

}

struct CudfHashJoin::State
{
    const GPUElementType key_element_type;
    const std::vector<GPUElementType> payload_element_types;

    cudf::table_view build_payloads;
    std::unique_ptr<cudf::hash_join> hash_join;
    bool built = false;

    State(GPUElementType key_element_type_, GPUSpan<GPUElementType> payload_element_types_)
        : key_element_type(integerKeyTypeOf(key_element_type_))
        , payload_element_types(payload_element_types_.begin(), payload_element_types_.end())
    {
    }
};

CudfHashJoin::CudfHashJoin(GPUElementType key_element_type, GPUSpan<GPUElementType> payload_element_types)
{
    initializeCudf();
    state = new State(key_element_type, payload_element_types);
}

CudfHashJoin::~CudfHashJoin()
{
    delete state;
}

void CudfHashJoin::build(DeviceFixedColumn keys, GPUSpan<DeviceFixedColumn> payloads)
{
    if (state->built)
        throwGPUError("the hash table was built twice");

    checkCount(payloads.size(), state->payload_element_types.size(), "payload columns of the right table");

    state->built = true;

    std::vector<cudf::column_view> payload_columns;
    payload_columns.reserve(payloads.size());
    for (size_t i = 0; i < payloads.size(); ++i)
    {
        checkCount(payloads[i].rows, keys.rows, "rows in payload column " + std::to_string(i));
        payload_columns.push_back(columnViewOf(payloads[i], state->payload_element_types[i], "payload column " + std::to_string(i)));
    }
    state->build_payloads = cudf::table_view(payload_columns);

    /// An empty right table needs no hash table: every probe of it matches nothing.
    if (keys.rows == 0)
        return;

    const rmm::cuda_stream_view stream = StreamRegistry::get().compute;
    const std::vector<cudf::column_view> key_columns{columnViewOf(keys, state->key_element_type, "the keys of the right table")};

    state->hash_join = guarded(
        "building a hash table over " + std::to_string(keys.rows) + " rows",
        [&] { return std::make_unique<cudf::hash_join>(cudf::table_view(key_columns), cudf::null_equality::EQUAL, stream); });
}

struct CudfHashJoinProbe::State
{
    const CudfHashJoin::State & join;
    const rmm::cuda_stream_view stream;

    std::unique_ptr<rmm::device_uvector<cudf::size_type>> probe_indices;
    std::unique_ptr<cudf::table> gathered_payloads;
    bool probed = false;

    State(const CudfHashJoin::State & join_, rmm::cuda_stream_view stream_)
        : join(join_)
        , stream(stream_)
    {
    }
};

CudfHashJoinProbe::CudfHashJoinProbe(const CudfHashJoin & join, rmm::cuda_stream_view stream)
{
    if (!join.state->built)
        throwGPUError("a probe of a hash table that is not built");

    state = new State(*join.state, stream);
}

/// The matches are freed in the order of the probe's stream, which the caller keeps until after.
CudfHashJoinProbe::~CudfHashJoinProbe()
{
    delete state;
}

size_t CudfHashJoinProbe::probe(DeviceFixedColumn keys)
{
    if (keys.rows == 0)
        throwGPUError("an empty block of the left table");

    state->gathered_payloads.reset();
    state->probe_indices.reset();
    state->probed = true;

    const CudfHashJoin::State & join = state->join;
    if (!join.hash_join)
        return 0;

    const rmm::cuda_stream_view stream = state->stream;
    const std::vector<cudf::column_view> key_columns{columnViewOf(keys, join.key_element_type, "the keys of a left block")};

    auto [probe_side, build_side] = guarded(
        "probing the hash table with " + std::to_string(keys.rows) + " rows",
        [&] { return join.hash_join->inner_join(cudf::table_view(key_columns), std::nullopt, stream); });

    if (probe_side->size() != build_side->size())
        throwGPUError(
            "the device returned " + std::to_string(probe_side->size()) + " probe-side indices against "
            + std::to_string(build_side->size()) + " build-side ones");

    /// The left block's own columns are indexed on the host, so only the right table's payload is
    /// gathered here.
    if (!join.payload_element_types.empty() && !build_side->is_empty())
    {
        const cudf::column_view build_index_column(
            cudf::data_type{cudf::type_id::INT32}, static_cast<cudf::size_type>(build_side->size()), build_side->data(), nullptr, 0);

        state->gathered_payloads = guarded(
            "gathering " + std::to_string(build_side->size()) + " rows of the right table",
            [&] { return cudf::gather(join.build_payloads, build_index_column, cudf::out_of_bounds_policy::DONT_CHECK, stream); });
    }

    const size_t num_matches = probe_side->size();
    state->probe_indices = std::move(probe_side);

    return num_matches;
}

DeviceFixedColumn CudfHashJoinProbe::probeRowIndices() const
{
    if (!state->probed)
        throwGPUError("a probe's result was asked for before the probe ran");

    static_assert(sizeof(cudf::size_type) == sizeof(uint32_t), "the probe-side row indices are handed back as they are");

    if (!state->probe_indices)
        return {GPUElementType::UInt32, nullptr, 0};

    return {GPUElementType::UInt32, reinterpret_cast<const char *>(state->probe_indices->data()), state->probe_indices->size()};
}

DeviceFixedColumn CudfHashJoinProbe::gatheredPayload(size_t index) const
{
    if (!state->probed)
        throwGPUError("a probe's result was asked for before the probe ran");

    const CudfHashJoin::State & join = state->join;
    if (index >= join.payload_element_types.size())
        throwGPUError("payload column " + std::to_string(index) + " of " + std::to_string(join.payload_element_types.size()));

    if (!state->gathered_payloads)
        return {join.payload_element_types[index], nullptr, 0};

    return deviceViewOf(
        state->gathered_payloads->view().column(static_cast<cudf::size_type>(index)),
        join.payload_element_types[index],
        "a gathered column of the right table");
}

}
