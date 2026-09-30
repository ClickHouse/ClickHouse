#include <GPU/OffsetsKernels.cuh>

#include <GPU/Cudf.cuh>

#include <rmm/exec_policy.hpp>

#include <thrust/binary_search.h>
#include <thrust/scan.h>
#include <thrust/transform.h>

#include <string>

namespace DB::GPU
{

namespace
{

struct SubtractFrom
{
    uint64_t minus;

    __device__ uint64_t operator()(uint64_t offset) const { return offset - minus; }
};

struct AddStart
{
    const uint64_t * start;

    __device__ uint64_t operator()(uint64_t running_size) const { return *start + running_size; }
};

}

__host__ void subtractFromOffsets(const uint64_t * from, size_t count, uint64_t minus, uint64_t * to, rmm::cuda_stream_view stream)
{
    if (count == 0 || (minus == 0 && from == to))
        return;

    guarded("rebasing " + std::to_string(count) + " string offsets", [&]
    {
        thrust::transform(rmm::exec_policy_nosync(stream), from, from + count, to, SubtractFrom{minus});
    });
}

__host__ void offsetsFromSizes(const uint64_t * sizes, size_t count, uint64_t * offsets_end, rmm::cuda_stream_view stream)
{
    if (count == 0)
        return;

    guarded("turning " + std::to_string(count) + " string sizes into offsets", [&]
    {
        const auto policy = rmm::exec_policy_nosync(stream);
        thrust::inclusive_scan(policy, sizes, sizes + count, offsets_end + 1);
        thrust::transform(policy, offsets_end + 1, offsets_end + 1 + count, offsets_end + 1, AddStart{offsets_end});
    });
}

__host__ CoveredRows rowsCoveredBy(const uint64_t * offsets, size_t num_rows, uint64_t chars_bytes, rmm::cuda_stream_view stream)
{
    return guarded("counting the strings within " + std::to_string(chars_bytes) + " bytes", [&]
    {
        const uint64_t * const ends = offsets + 1;
        const uint64_t * const past = thrust::upper_bound(rmm::exec_policy(stream), ends, ends + num_rows, chars_bytes);

        CoveredRows covered{.rows = static_cast<size_t>(past - ends)};
        checkCuda(
            cudaMemcpyAsync(&covered.bytes, offsets + covered.rows, sizeof(uint64_t), cudaMemcpyDeviceToHost, stream.value()),
            "cannot copy an offset back");
        checkCuda(cudaStreamSynchronize(stream.value()), "cannot wait for an offset to come back");
        return covered;
    });
}

}
