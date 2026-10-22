#include <GPU/CudfHashJoin.cuh>

#include <GPU/Cudf.cuh>

#include <cudf/join/hash_join.hpp>
#include <cudf/table/table_view.hpp>

#include <rmm/device_uvector.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace DB::GPU
{

struct CudfJoinMatches::State
{
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> probe_indices;
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> build_indices;
};

__host__ CudfJoinMatches::CudfJoinMatches(CudfJoinMatches && other) noexcept
    : state(std::exchange(other.state, nullptr))
{
}

__host__ CudfJoinMatches & CudfJoinMatches::operator=(CudfJoinMatches && other) noexcept
{
    std::swap(state, other.state);
    return *this;
}

__host__ CudfJoinMatches::~CudfJoinMatches()
{
    delete state;
}

__host__ size_t CudfJoinMatches::size() const
{
    return state ? state->probe_indices->size() : 0;
}

__host__ void CudfJoinMatches::copyOut(uint32_t * probe_indices, uint32_t * build_indices, cuda::stream_ref stream) const
{
    static_assert(sizeof(cudf::size_type) == sizeof(uint32_t), "the row indices of cuDF are copied out as they are");

    const size_t num_matches = size();
    if (num_matches == 0)
        return;

    checkCuda(
        cudaMemcpyAsync(probe_indices, state->probe_indices->data(), num_matches * sizeof(uint32_t), cudaMemcpyDeviceToHost, stream.get()),
        "Cannot copy the probe-side row indices of a join back");
    checkCuda(
        cudaMemcpyAsync(build_indices, state->build_indices->data(), num_matches * sizeof(uint32_t), cudaMemcpyDeviceToHost, stream.get()),
        "Cannot copy the build-side row indices of a join back");
    checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for the row indices of a join");
}

struct CudfHashJoin::State
{
    const GPUElementType key_type;
    /// None for an empty right table, which no probe matches.
    std::unique_ptr<cudf::hash_join> hash_join;

    explicit State(GPUElementType key_type_) : key_type(key_type_) { }
};

__host__ CudfHashJoin::CudfHashJoin(GPUElementType key_type, DeviceFixedColumn build_keys)
{
    if (!isInteger(key_type))
        throwGPUError(("a join key of element type " + std::to_string(static_cast<int>(key_type)) + ", which is not an integer").c_str());

    auto new_state = std::make_unique<State>(key_type);
    if (build_keys.rows != 0)
    {
        const std::vector<cudf::column_view> keys{columnViewOf(build_keys, key_type)};
        const cuda::stream_ref stream = GPUResources::get().compute;
        new_state->hash_join = guarded(
            "building a hash table of " + std::to_string(build_keys.rows) + " keys",
            [&] { return std::make_unique<cudf::hash_join>(cudf::table_view(keys), cudf::null_equality::EQUAL, stream); });
        /// The probes run on streams of their own.
        checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for the hash table of a join");
    }
    state = new_state.release();
}

__host__ CudfHashJoin::~CudfHashJoin()
{
    delete state;
}

__host__ CudfJoinMatches CudfHashJoin::probe(DeviceFixedColumn probe_keys, cuda::stream_ref stream) const
{
    CudfJoinMatches matches;
    if (!state->hash_join || probe_keys.rows == 0)
        return matches;

    const std::vector<cudf::column_view> keys{columnViewOf(probe_keys, state->key_type)};
    auto [probe_side, build_side] = guarded(
        "probing a hash table with " + std::to_string(probe_keys.rows) + " keys",
        [&] { return state->hash_join->inner_join(cudf::table_view(keys), std::nullopt, stream); });

    if (probe_side->size() != build_side->size())
        throwGPUError(
            ("the device returned " + std::to_string(probe_side->size()) + " probe-side indices against "
             + std::to_string(build_side->size()) + " build-side ones").c_str());

    matches.state = new CudfJoinMatches::State{.probe_indices = std::move(probe_side), .build_indices = std::move(build_side)};
    return matches;
}

}
