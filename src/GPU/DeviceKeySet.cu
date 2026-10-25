#include <GPU/DeviceKeySet.h>

#include <GPU/Cudf.cuh>
#include <GPU/GPUFilter.cuh>

#include <cuco/static_set.cuh>
#include <rmm/device_uvector.hpp>
#include <rmm/exec_policy.hpp>
#include <thrust/count.h>
#include <thrust/remove.h>

#include <memory>
#include <string>

namespace DB::GPU
{

namespace
{

constexpr int threads_per_block = 256;

/// No key of fewer than 64 bits widens to it, and a 64-bit key that is it is kept apart, see `State::has_sentinel`.
constexpr uint64_t sentinel = ~uint64_t{0};

/// The keys as 64-bit integers, the signed ones sign-extended, so that a key of either table widens to the same bits.
__global__ void widenKeys(const char * data, GPUElementType type, size_t rows, uint64_t * out)
{
    const size_t row = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    if (row < rows)
        out[row] = static_cast<uint64_t>(Filtering::loadInteger(data, row, type));
}

__global__ void markSentinel(const uint64_t * widened, size_t rows, uint8_t * out)
{
    const size_t row = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    if (row < rows && widened[row] == sentinel)
        out[row] = 1;
}

unsigned blocksFor(size_t rows)
{
    return static_cast<unsigned>((rows + threads_per_block - 1) / threads_per_block);
}

rmm::device_uvector<uint64_t> widened(DeviceFixedColumn keys, cuda::stream_ref stream)
{
    rmm::device_uvector<uint64_t> out(keys.rows, stream);
    if (keys.rows != 0)
    {
        widenKeys<<<blocksFor(keys.rows), threads_per_block, 0, stream.get()>>>(keys.data, keys.element_type, keys.rows, out.data());
        checkCuda(cudaGetLastError(), "Cannot launch the widening of keys");
    }
    return out;
}

}

struct DeviceKeySet::State
{
    using Set = cuco::static_set<uint64_t>;

    const GPUElementType type;
    std::unique_ptr<Set> set;
    /// Whether the keys hold the value the set keeps for an empty slot, which is kept out of it and matched apart.
    bool has_sentinel = false;

    explicit State(GPUElementType type_) : type(type_) { }
};

DeviceKeySet::DeviceKeySet(DeviceFixedColumn keys, cuda::stream_ref stream)
{
    if (!isInteger(keys.element_type))
        throwGPUError(("a set of keys of element type " + std::to_string(static_cast<int>(keys.element_type)) + ", which is not an integer").c_str());

    auto new_state = std::make_unique<State>(keys.element_type);
    rmm::device_uvector<uint64_t> all = widened(keys, stream);

    auto end = all.end();
    if (sizeOf(keys.element_type) == 8 && keys.rows != 0)
    {
        end = thrust::remove(rmm::exec_policy(stream), all.begin(), all.end(), sentinel);
        new_state->has_sentinel = end != all.end();
    }
    const size_t num_keys = static_cast<size_t>(end - all.begin());

    new_state->set = guarded("building a set of " + std::to_string(num_keys) + " keys", [&]
    {
        std::unique_ptr<State::Set> set(new State::Set(
            cuco::extent<size_t>{std::max<size_t>(num_keys, 1)}, 0.5, cuco::empty_key<uint64_t>{sentinel}, {}, {}, {}, {}, {}, stream));
        if (num_keys != 0)
            set->insert_async(all.begin(), end, stream);
        return set;
    });
    checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for a set of keys to be built");

    state = new_state.release();
}

DeviceKeySet::~DeviceKeySet()
{
    delete state;
}

GPUElementType DeviceKeySet::keyType() const
{
    return state->type;
}

void DeviceKeySet::contains(DeviceFixedColumn needles, uint8_t * out, cuda::stream_ref stream) const
{
    if (needles.element_type != state->type)
        throwGPUError("keys of another type than the set's");
    if (needles.rows == 0)
        return;

    rmm::device_uvector<uint64_t> all = widened(needles, stream);
    guarded("looking " + std::to_string(needles.rows) + " keys up in a set", [&]
    {
        state->set->contains_async(all.begin(), all.end(), reinterpret_cast<bool *>(out), stream);
        return 0;
    });
    if (state->has_sentinel)
    {
        markSentinel<<<blocksFor(needles.rows), threads_per_block, 0, stream.get()>>>(all.data(), needles.rows, out);
        checkCuda(cudaGetLastError(), "Cannot launch the matching of the sentinel key");
    }
    /// `all` is freed in the order of `stream`, after the lookups that read it.
}

}
