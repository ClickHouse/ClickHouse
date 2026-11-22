#include <GPU/CuvsIvf.cuh>
#include <GPU/GPUTypes.cuh>
#include <GPU/IvfQuantization.cuh>

#include <cub/device/device_radix_sort.cuh>
#include <cuvs/selection/select_k.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resource/device_memory_resource.hpp>
#include <raft/core/resources.hpp>
#include <raft/linalg/gemm.cuh>
#include <raft/matrix/detail/select_warpsort.cuh>
#include <raft/matrix/init.cuh>
#include <rmm/device_uvector.hpp>
#include <rmm/error.hpp>

#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <vector>

namespace DB::GPU
{
namespace
{
constexpr size_t SCORES_BYTES = 64 << 20;

constexpr size_t WORKSPACE_BYTES = 256 << 20;

constexpr uint32_t MISSING_ROW = std::numeric_limits<uint32_t>::max();

constexpr int SCAN_THREADS = 256;

constexpr int MAX_SELECT = 256;

constexpr size_t PARTIAL_RESULTS_BYTES = 256 << 20;

using RowMatrix = raft::device_matrix_view<float, int64_t, raft::row_major>;
using ConstRowMatrix = raft::device_matrix_view<const float, int64_t, raft::row_major>;

void limitWorkspace(raft::resources & res)
{
    raft::resource::set_workspace_to_global_resource(res, WORKSPACE_BYTES);
}

template <typename Body>
auto translateErrors(const char * doing, Body && body)
{
    try
    {
        return body();
    }
    catch (const rmm::out_of_memory & exception)
    {
        throwGPUError((std::string(doing) + ": out of device memory: " + exception.what()).c_str());
    }
    catch (const std::exception & exception)
    {
        if (isClickHouseException(exception))
            throw;
        throwGPUError((std::string(doing) + ": " + exception.what()).c_str());
    }
}

cuda::stream_ref streamOf(raft::resources & res)
{
    return raft::resource::get_cuda_stream(res);
}

raft::resources & searchResources()
{
    thread_local raft::resources resources = []
    {
        raft::resources res;
        limitWorkspace(res);
        return res;
    }();
    return resources;
}

/// Rows `x` (n x d) times R^T, that is R x of each row, into `out`. `rotation` is R, row-major, read as R^T column-major.
void rotateRows(raft::resources & res, float * x, size_t n, float * rotation, size_t d, float * out)
{
    raft::linalg::gemm(res,
        raft::make_device_matrix_view<float, int64_t, raft::row_major>(x, int64_t(n), int64_t(d)),
        raft::make_device_matrix_view<float, int64_t, raft::col_major>(rotation, int64_t(d), int64_t(d)),
        raft::make_device_matrix_view<float, int64_t, raft::row_major>(out, int64_t(n), int64_t(d)));
}

void fillMissing(raft::resources & res, float * distances, uint32_t * rows, size_t size)
{
    raft::matrix::fill(res, raft::make_device_vector_view<float, int64_t>(distances, int64_t(size)), std::numeric_limits<float>::infinity());
    raft::matrix::fill(res, raft::make_device_vector_view<uint32_t, int64_t>(rows, int64_t(size)), MISSING_ROW);
}

}

namespace
{

constexpr int ROUTE_MAX_BEAM = 8;
constexpr int ROUTE_WARPS_PER_BLOCK = 4;

/// The `take` smallest of the scores the lanes of a warp hold, `local_count` of them sorted in each lane, into `chosen`, the same
/// in every lane. Returns how many there were.
__device__ int warpSmallest(const float * local_scores, const uint32_t * local_nodes, int local_count, int take, uint32_t * chosen)
{
    int head = 0;
    int count = 0;
    for (; count < take; ++count)
    {
        float score = head < local_count ? local_scores[head] : INFINITY;
        uint32_t node = head < local_count ? local_nodes[head] : 0xFFFFFFFFu;
        for (int offset = 16; offset > 0; offset /= 2)
        {
            const float other_score = __shfl_xor_sync(0xFFFFFFFFu, score, offset);
            const uint32_t other_node = __shfl_xor_sync(0xFFFFFFFFu, node, offset);
            if (other_score < score || (other_score == score && other_node < node))
            {
                score = other_score;
                node = other_node;
            }
        }
        if (node == 0xFFFFFFFFu)
            break;
        chosen[count] = node;
        if (head < local_count && local_nodes[head] == node)
            ++head;
    }
    return count;
}

/// A warp per row, the row `order[w]` of warp `w` (or `w` without `order`): descends the levels [first_level, end_level),
/// keeping on each the `beam` nodes nearest to the row among the children of the nodes kept on the level above (all nodes of
/// the top level), then writes the nearest leaf, or, above the leaves, the nodes kept to `frontiers` (`ROUTE_MAX_BEAM` per
/// row) and their number to `frontier_sizes`, which a level below the top also starts from. Scores are |c|^2 - 2 x c,
/// which order as the L2 distance.
__global__ void routeRows(const float * vectors, size_t rows, size_t dimensions, const float * centroids, const float * squared_lengths,
    const uint32_t * child_first, const uint32_t * level_begin, uint32_t first_level, uint32_t end_level, uint32_t num_levels, int beam,
    const uint32_t * order, uint32_t * frontiers, uint8_t * frontier_sizes, uint32_t * leaves)
{
    extern __shared__ float shared_rows[];
    const int warp = int(threadIdx.x) / 32;
    const int lane = int(threadIdx.x) % 32;
    const size_t index = size_t(blockIdx.x) * ROUTE_WARPS_PER_BLOCK + warp;
    if (index >= rows)
        return;
    const size_t row = order ? order[index] : index;
    float * x = shared_rows + size_t(warp) * dimensions;
    for (size_t i = lane; i < dimensions; i += 32)
        x[i] = vectors[row * dimensions + i];
    __syncwarp();

    uint32_t frontier[ROUTE_MAX_BEAM];
    int frontier_size = 0;
    if (first_level > 0)
    {
        frontier_size = frontier_sizes[row];
        for (int k = 0; k < frontier_size; ++k)
            frontier[k] = frontiers[row * ROUTE_MAX_BEAM + k];
    }
    float local_scores[ROUTE_MAX_BEAM];
    uint32_t local_nodes[ROUTE_MAX_BEAM];

    for (uint32_t level = first_level; level < end_level; ++level)
    {
        const bool is_leaves = level + 1 == num_levels;
        const int keep = is_leaves ? 1 : beam;
        int local_count = 0;

        /// The candidates: all nodes of the top level, else the children of the frontier. The warp reads each centroid
        /// together, a lane per 32nd component, so that its reads are adjacent; the lane of every 32nd candidate keeps it.
        const int num_ranges = level == 0 ? 1 : frontier_size;
        uint32_t seen = 0;
        for (int r = 0; r < num_ranges; ++r)
        {
            const uint32_t first = level == 0 ? level_begin[0] : child_first[frontier[r]];
            const uint32_t last = level == 0 ? level_begin[1] : child_first[frontier[r] + 1];
            /// Four candidates at a time, whose sums do not wait for one another.
            for (uint32_t group = first; group < last; group += 4)
            {
                float dots[4] = {0, 0, 0, 0};
                const uint32_t group_size = min(4u, last - group);
                for (size_t i = lane; i < dimensions; i += 32)
                {
                    const float component = x[i];
#pragma unroll
                    for (uint32_t k = 0; k < 4; ++k)
                        if (k < group_size)
                            dots[k] += component * centroids[size_t(group + k) * dimensions + i];
                }
#pragma unroll
                for (int offset = 16; offset > 0; offset /= 2)
#pragma unroll
                    for (uint32_t k = 0; k < 4; ++k)
                        dots[k] += __shfl_xor_sync(0xFFFFFFFFu, dots[k], offset);

                for (uint32_t k = 0; k < group_size; ++k)
                {
                    const uint32_t c = group + k;
                    if (int((seen + c - first) % 32) != lane)
                        continue;
                    const float score = squared_lengths[c] - 2 * dots[k];
                    if (local_count < keep || score < local_scores[local_count - 1])
                    {
                        int at = local_count < keep ? local_count++ : keep - 1;
                        while (at > 0 && local_scores[at - 1] > score)
                        {
                            local_scores[at] = local_scores[at - 1];
                            local_nodes[at] = local_nodes[at - 1];
                            --at;
                        }
                        local_scores[at] = score;
                        local_nodes[at] = c;
                    }
                }
            }
            seen += last - first;
        }

        frontier_size = warpSmallest(local_scores, local_nodes, local_count, keep, frontier);
        if (is_leaves)
        {
            if (lane == 0)
                leaves[row] = frontier_size ? frontier[0] - level_begin[level] : MISSING_ROW;
            return;
        }
    }

    if (lane == 0)
    {
        frontier_sizes[row] = uint8_t(frontier_size);
        for (int k = 0; k < frontier_size; ++k)
            frontiers[row * ROUTE_MAX_BEAM + k] = frontier[k];
    }
}

/// The key of a row for the order of the last level: its nearest node above the leaves.
__global__ void frontierKeys(const uint32_t * frontiers, const uint8_t * frontier_sizes, size_t rows, uint32_t * keys, uint32_t * indices)
{
    const size_t row = size_t(blockIdx.x) * blockDim.x + threadIdx.x;
    if (row >= rows)
        return;
    keys[row] = frontier_sizes[row] ? frontiers[row * ROUTE_MAX_BEAM] : 0xFFFFFFFFu;
    indices[row] = uint32_t(row);
}

/// A warp per row: the record of its entry, see `IvfTreeRouter::assignAndEncode` and `IvfQuantization::encode`.
__global__ void encodeRows(const float * rotated, const uint32_t * leaves, const float * rotated_leaves, size_t rows, size_t dimensions,
    uint32_t first_row, size_t record_bytes, char * records)
{
    const int warp = int(threadIdx.x) / 32;
    const int lane = int(threadIdx.x) % 32;
    const size_t row = size_t(blockIdx.x) * ROUTE_WARPS_PER_BLOCK + warp;
    if (row >= rows)
        return;
    const uint32_t leaf = leaves[row];
    if (leaf == MISSING_ROW)
    {
        if (lane == 0)
            memcpy(records + row * record_bytes, &leaf, sizeof(leaf));
        return;
    }
    const float * x = rotated + row * dimensions;
    const float * c = rotated_leaves + size_t(leaf) * dimensions;

    float lo = INFINITY;
    float hi = -INFINITY;
    for (size_t i = lane; i < dimensions; i += 32)
    {
        const float r = x[i] - c[i];
        lo = fminf(lo, r);
        hi = fmaxf(hi, r);
    }
    for (int offset = 16; offset > 0; offset /= 2)
    {
        lo = fminf(lo, __shfl_xor_sync(0xFFFFFFFFu, lo, offset));
        hi = fmaxf(hi, __shfl_xor_sync(0xFFFFFFFFu, hi, offset));
    }
    const float step = hi > lo ? (hi - lo) / 15 : 1.0f;

    char * record = records + row * record_bytes;
    uint32_t * codes = reinterpret_cast<uint32_t *>(record + 2 * sizeof(uint32_t) + sizeof(IvfQuantization::CodeParams));
    float squared_length = 0;
    const size_t words = IvfQuantization::codeWords(dimensions);
    for (size_t w = 0; w < words; ++w)
    {
        const size_t i = w * 32 + size_t(lane);
        uint32_t code = 0;
        if (i < dimensions)
        {
            code = uint32_t(min(max(int(lroundf((x[i] - c[i] - lo) / step)), 0), 15));
            const float decoded = lo + float(code) * step;
            squared_length += decoded * decoded;
        }
        /// Component i is the nibble i % 8 of the 32-bit part (i % 32) / 8 of the word i / 32.
        uint32_t part = code << (4 * (lane % 8));
        part |= __shfl_xor_sync(0xFFFFFFFFu, part, 1);
        part |= __shfl_xor_sync(0xFFFFFFFFu, part, 2);
        part |= __shfl_xor_sync(0xFFFFFFFFu, part, 4);
        if (lane % 8 == 0)
            codes[w * 4 + size_t(lane / 8)] = part;
    }
    for (int offset = 16; offset > 0; offset /= 2)
        squared_length += __shfl_xor_sync(0xFFFFFFFFu, squared_length, offset);

    if (lane == 0)
    {
        const uint32_t row_number = first_row + uint32_t(row);
        const IvfQuantization::CodeParams params{lo, step, squared_length};
        memcpy(record, &leaf, sizeof(leaf));
        memcpy(record + sizeof(uint32_t), &row_number, sizeof(row_number));
        memcpy(record + 2 * sizeof(uint32_t), &params, sizeof(params));
    }
}

}

struct IvfTreeRouter::State
{
    size_t dimensions;
    uint32_t num_levels;
    std::vector<uint32_t> level_begin_host;
    mutable raft::resources resources;
    rmm::device_uvector<float> centroids;
    rmm::device_uvector<float> squared_lengths;
    rmm::device_uvector<uint32_t> child_first;
    rmm::device_uvector<uint32_t> level_begin;
    rmm::device_uvector<float> rotation;
    rmm::device_uvector<float> rotated_leaves;

    State(size_t dimensions_, size_t num_levels_, const size_t * nodes, const float * const * host_centroids,
        const uint32_t * const * child_offsets, const float * host_rotation, const float * host_rotated_leaves)
        : dimensions(dimensions_)
        , num_levels(uint32_t(num_levels_))
        , centroids(0, streamOf(resources))
        , squared_lengths(0, streamOf(resources))
        , child_first(0, streamOf(resources))
        , level_begin(0, streamOf(resources))
        , rotation(dimensions_ * dimensions_, streamOf(resources))
        , rotated_leaves(nodes[num_levels_ - 1] * dimensions_, streamOf(resources))
    {
        limitWorkspace(resources);
        const auto stream_view = streamOf(resources);
        const cudaStream_t stream = stream_view.get();

        level_begin_host.assign(num_levels + 1, 0);
        for (uint32_t l = 0; l < num_levels; ++l)
            level_begin_host[l + 1] = level_begin_host[l] + uint32_t(nodes[l]);
        const size_t total = level_begin_host[num_levels];

        std::vector<float> all_centroids(total * dimensions);
        std::vector<float> all_lengths(total);
        /// The children of node `g` are [child_first[g], child_first[g + 1]) of all nodes; the offsets of a level end where
        /// those of the next begin.
        std::vector<uint32_t> all_child_first(level_begin_host[num_levels - 1] + 1, 0);
        for (uint32_t l = 0; l < num_levels; ++l)
        {
            std::copy(host_centroids[l], host_centroids[l] + nodes[l] * dimensions, all_centroids.begin() + size_t(level_begin_host[l]) * dimensions);
            if (l + 1 < num_levels)
                for (size_t i = 0; i <= nodes[l]; ++i)
                    all_child_first[level_begin_host[l] + i] = level_begin_host[l + 1] + child_offsets[l][i];
        }
        for (size_t g = 0; g < total; ++g)
        {
            double sum = 0;
            for (size_t i = 0; i < dimensions; ++i)
                sum += double(all_centroids[g * dimensions + i]) * double(all_centroids[g * dimensions + i]);
            all_lengths[g] = float(sum);
        }

        centroids.resize(all_centroids.size(), stream_view);
        squared_lengths.resize(all_lengths.size(), stream_view);
        child_first.resize(all_child_first.size(), stream_view);
        level_begin.resize(level_begin_host.size(), stream_view);
        checkCuda(cudaMemcpyAsync(centroids.data(), all_centroids.data(), all_centroids.size() * sizeof(float), cudaMemcpyHostToDevice, stream),
            "copying the tree of an IVF index to the device");
        checkCuda(cudaMemcpyAsync(squared_lengths.data(), all_lengths.data(), all_lengths.size() * sizeof(float), cudaMemcpyHostToDevice, stream),
            "copying the tree of an IVF index to the device");
        checkCuda(cudaMemcpyAsync(child_first.data(), all_child_first.data(), all_child_first.size() * sizeof(uint32_t), cudaMemcpyHostToDevice, stream),
            "copying the tree of an IVF index to the device");
        checkCuda(cudaMemcpyAsync(level_begin.data(), level_begin_host.data(), level_begin_host.size() * sizeof(uint32_t), cudaMemcpyHostToDevice, stream),
            "copying the tree of an IVF index to the device");
        checkCuda(cudaMemcpyAsync(rotation.data(), host_rotation, rotation.size() * sizeof(float), cudaMemcpyHostToDevice, stream),
            "copying the rotation to the device");
        checkCuda(cudaMemcpyAsync(rotated_leaves.data(), host_rotated_leaves, rotated_leaves.size() * sizeof(float), cudaMemcpyHostToDevice,
            stream), "copying the rotated leaves of an IVF index to the device");
        raft::resource::sync_stream(resources);
    }
};

__host__ IvfTreeRouter::IvfTreeRouter(size_t dimensions, size_t num_levels, const size_t * nodes, const float * const * centroids,
    const uint32_t * const * child_offsets, const float * rotation, const float * rotated_leaves)
{
    if (num_levels == 0)
        throwGPUError("the tree of an IVF index has no levels");
    translateErrors("uploading the tree of an IVF index", [&]
    {
        state = new State(dimensions, num_levels, nodes, centroids, child_offsets, rotation, rotated_leaves);
    });
}

__host__ IvfTreeRouter::~IvfTreeRouter()
{
    delete state;
}

__host__ size_t IvfTreeRouter::maxBeam()
{
    return ROUTE_MAX_BEAM;
}

__host__ size_t IvfTreeRouter::recordBytes(size_t dimensions)
{
    return 2 * sizeof(uint32_t) + sizeof(IvfQuantization::CodeParams) + IvfQuantization::codeBytes(dimensions);
}

__host__ void IvfTreeRouter::assignAndEncode(const float * vectors, size_t rows, size_t beam, uint32_t first_row, char * records) const
{
    if (beam == 0 || beam > size_t(ROUTE_MAX_BEAM))
        throwGPUError("the beam of the assignment of rows to the lists of an IVF index is out of range");
    translateErrors("assigning rows to the lists of an IVF index", [&]
    {
        auto & res = state->resources;
        const auto stream_view = streamOf(res);
        const cudaStream_t stream = stream_view.get();

        const size_t d = state->dimensions;
        const size_t record_bytes = recordBytes(d);
        const size_t shared_bytes = ROUTE_WARPS_PER_BLOCK * d * sizeof(float);
        if (shared_bytes > 48 * 1024)
            checkCuda(cudaFuncSetAttribute(routeRows, cudaFuncAttributeMaxDynamicSharedMemorySize, int(shared_bytes)),
                "allowing the shared memory of the assignment of IVF rows");

        const size_t chunk = std::clamp<size_t>(SCORES_BYTES / (d * sizeof(float)), 1, std::max<size_t>(rows, 1));
        rmm::device_uvector<float> device_vectors(chunk * d, stream_view);
        rmm::device_uvector<float> device_rotated(chunk * d, stream_view);
        rmm::device_uvector<uint32_t> device_leaves(chunk, stream_view);
        rmm::device_uvector<char> device_records(chunk * record_bytes, stream_view);
        rmm::device_uvector<uint32_t> device_frontiers(chunk * ROUTE_MAX_BEAM, stream_view);
        rmm::device_uvector<uint8_t> device_frontier_sizes(chunk, stream_view);
        rmm::device_uvector<uint32_t> device_keys(chunk, stream_view);
        rmm::device_uvector<uint32_t> device_sorted_keys(chunk, stream_view);
        rmm::device_uvector<uint32_t> device_indices(chunk, stream_view);
        rmm::device_uvector<uint32_t> device_order(chunk, stream_view);
        size_t sort_bytes = 0;
        checkCuda(cub::DeviceRadixSort::SortPairs(nullptr, sort_bytes, device_keys.data(), device_sorted_keys.data(), device_indices.data(),
            device_order.data(), int(chunk), 0, 32, stream), "sizing the order of IVF rows");
        rmm::device_buffer sort_storage(sort_bytes, stream_view);
        for (size_t begin = 0; begin < rows; begin += chunk)
        {
            const size_t n = std::min(chunk, rows - begin);
            checkCuda(cudaMemcpyAsync(device_vectors.data(), vectors + begin * d, n * d * sizeof(float), cudaMemcpyHostToDevice, stream),
                "copying vectors to the device");
            const size_t blocks = (n + ROUTE_WARPS_PER_BLOCK - 1) / ROUTE_WARPS_PER_BLOCK;
            const uint32_t levels = state->num_levels;
            if (levels == 1)
                routeRows<<<blocks, ROUTE_WARPS_PER_BLOCK * 32, shared_bytes, stream>>>(device_vectors.data(), n, d,
                    state->centroids.data(), state->squared_lengths.data(), state->child_first.data(), state->level_begin.data(), 0, 1, 1,
                    int(beam), nullptr, nullptr, nullptr, device_leaves.data());
            else
            {
                /// Above the leaves in the order of the rows, then the leaves in the order of the nearest nodes above them, so
                /// that warps nearby read the same leaves.
                routeRows<<<blocks, ROUTE_WARPS_PER_BLOCK * 32, shared_bytes, stream>>>(device_vectors.data(), n, d,
                    state->centroids.data(), state->squared_lengths.data(), state->child_first.data(), state->level_begin.data(), 0,
                    levels - 1, levels, int(beam), nullptr, device_frontiers.data(), device_frontier_sizes.data(), nullptr);
                checkCuda(cudaGetLastError(), "launching the assignment of IVF rows");
                frontierKeys<<<(n + 255) / 256, 256, 0, stream>>>(device_frontiers.data(), device_frontier_sizes.data(), n,
                    device_keys.data(), device_indices.data());
                checkCuda(cudaGetLastError(), "launching the order of IVF rows");
                size_t sort_capacity = sort_storage.size();
                checkCuda(cub::DeviceRadixSort::SortPairs(sort_storage.data(), sort_capacity, device_keys.data(), device_sorted_keys.data(),
                    device_indices.data(), device_order.data(), int(n), 0, 32, stream), "ordering IVF rows");
                routeRows<<<blocks, ROUTE_WARPS_PER_BLOCK * 32, shared_bytes, stream>>>(device_vectors.data(), n, d,
                    state->centroids.data(), state->squared_lengths.data(), state->child_first.data(), state->level_begin.data(),
                    levels - 1, levels, levels, int(beam), device_order.data(), device_frontiers.data(), device_frontier_sizes.data(),
                    device_leaves.data());
            }
            checkCuda(cudaGetLastError(), "launching the assignment of IVF rows");
            rotateRows(res, device_vectors.data(), n, state->rotation.data(), d, device_rotated.data());
            encodeRows<<<blocks, ROUTE_WARPS_PER_BLOCK * 32, 0, stream>>>(device_rotated.data(), device_leaves.data(),
                state->rotated_leaves.data(), n, d, first_row + uint32_t(begin), record_bytes, device_records.data());
            checkCuda(cudaGetLastError(), "launching the encoding of IVF rows");
            checkCuda(cudaMemcpyAsync(records + begin * record_bytes, device_records.data(), n * record_bytes, cudaMemcpyDeviceToHost, stream),
                "copying the records of the rows to the host");
            raft::resource::sync_stream(res);
        }
    });
}

namespace
{

/// u = R q - R c of the query `q` and the list `list` into `u`, with its squared length and sum. Called by the whole block.
__device__ IvfQuantization::QueryResidual computeQueryResidual(const float * rotated_queries, const float * rotated_centroids, size_t q,
    uint32_t list, size_t dimensions, float * u)
{
    __shared__ float squared_length;
    __shared__ float sum;
    for (size_t i = threadIdx.x; i < IvfQuantization::codeWords(dimensions) * 32; i += blockDim.x)
        u[i] = i < dimensions ? rotated_queries[q * dimensions + i] - rotated_centroids[size_t(list) * dimensions + i] : 0.0f;
    __syncthreads();
    if (threadIdx.x == 0)
    {
        float s = 0;
        float l = 0;
        for (size_t i = 0; i < dimensions; ++i)
        {
            l += u[i] * u[i];
            s += u[i];
        }
        squared_length = l;
        sum = s;
    }
    __syncthreads();
    return {u, squared_length, sum};
}

/// Block `b` keeps the best `select` rows of the lists `query_lists[block_first[b] .. block_last[b])` for the query `block_query[b]`,
/// starting from `in` if it is set, and writes them to `out + out_offsets[b]`.
template <int Capacity>
__global__ void scanChunk(const char * chunk, const uint64_t * list_begins, const uint32_t * list_sizes, const float * rotated_centroids,
    const uint32_t * block_query, const uint32_t * block_first, const uint32_t * block_last, const uint32_t * query_lists,
    const float * rotated_queries, size_t dimensions, size_t select, size_t queue_bytes,
    const float * in_distances, const uint32_t * in_rows, const uint64_t * out_offsets, float * out_distances, uint32_t * out_rows)
{
    extern __shared__ __align__(256) uint8_t shared[];
    float * u = reinterpret_cast<float *>(shared + queue_bytes);

    const size_t q = block_query[blockIdx.x];
    const uint32_t first = block_first[blockIdx.x];
    const uint32_t last = block_last[blockIdx.x];
    if (first == last)
        return;

    raft::matrix::detail::select::warpsort::block_sort<raft::matrix::detail::select::warpsort::warp_sort_filtered, Capacity, true,
        float, uint32_t> queue(static_cast<int>(select));

    /// `add` is called by every thread of a warp together, so the loops run to a multiple of the block.
    if (in_distances)
        for (size_t i = threadIdx.x; i < (select + blockDim.x - 1) / blockDim.x * blockDim.x; i += blockDim.x)
            queue.add(i < select ? in_distances[q * select + i] : INFINITY, i < select ? in_rows[q * select + i] : MISSING_ROW);

    const size_t words = IvfQuantization::codeWords(dimensions);
    for (uint32_t p = first; p < last; ++p)
    {
        const uint32_t list_number = query_lists[p];
        const auto query = computeQueryResidual(rotated_queries, rotated_centroids, q, list_number, dimensions, u);
        const IvfQuantization::ListView list(chunk + list_begins[list_number], list_sizes[list_number]);
        for (uint32_t start = 0; start < list.size; start += blockDim.x)
        {
            const uint32_t i = start + threadIdx.x;
            if (i < list.size)
                queue.add(IvfQuantization::estimateDistance(query, list.params[i], IvfQuantization::dotWithCodes(u, list.codes + i * words, words)),
                    list.rows[i]);
            else
                queue.add(INFINITY, MISSING_ROW);
        }
        __syncthreads();
    }

    queue.done(shared);
    queue.store(out_distances + out_offsets[blockIdx.x], out_rows + out_offsets[blockIdx.x]);
}

template <int Capacity>
void launchScan(size_t blocks, size_t dimensions, size_t select, cudaStream_t stream, const char * chunk, const uint64_t * list_begins,
    const uint32_t * list_sizes, const float * rotated_centroids, const uint32_t * block_query, const uint32_t * block_first,
    const uint32_t * block_last, const uint32_t * query_lists, const float * rotated_queries, const float * in_distances,
    const uint32_t * in_rows, const uint64_t * out_offsets, float * out_distances, uint32_t * out_rows)
{
    if constexpr (Capacity < MAX_SELECT)
    {
        if (select > size_t(Capacity))
            return launchScan<Capacity * 2>(blocks, dimensions, select, stream, chunk, list_begins, list_sizes, rotated_centroids,
                block_query, block_first, block_last, query_lists, rotated_queries, in_distances, in_rows, out_offsets, out_distances,
                out_rows);
    }
    const size_t queue_bytes = (raft::matrix::detail::select::warpsort::calc_smem_size_for_block_wide<float, uint32_t>(
        SCAN_THREADS / 32, int(select)) + 255) / 256 * 256;
    const size_t shared_bytes = queue_bytes + IvfQuantization::codeWords(dimensions) * 32 * sizeof(float);
    if (shared_bytes > 48 * 1024)
        checkCuda(cudaFuncSetAttribute(scanChunk<Capacity>, cudaFuncAttributeMaxDynamicSharedMemorySize, int(shared_bytes)),
            "allowing the shared memory of the scan of IVF lists");
    scanChunk<Capacity><<<blocks, SCAN_THREADS, shared_bytes, stream>>>(chunk, list_begins, list_sizes, rotated_centroids, block_query,
        block_first, block_last, query_lists, rotated_queries, dimensions, select, queue_bytes, in_distances, in_rows, out_offsets,
        out_distances, out_rows);
    checkCuda(cudaGetLastError(), "launching the scan of IVF lists");
}

}

struct IvfListScan::State
{
    size_t num_queries;
    size_t dimensions;
    size_t select;
    raft::resources & res;
    rmm::device_uvector<float> rotated_queries;
    rmm::device_uvector<float> best_distances;
    rmm::device_uvector<uint32_t> best_rows;

    State(const float * host_queries, size_t num_queries_, size_t dimensions_, size_t select_, const float * rotation)
        : num_queries(num_queries_)
        , dimensions(dimensions_)
        , select(select_)
        , res(searchResources())
        , rotated_queries(num_queries_ * dimensions_, streamOf(res))
        , best_distances(num_queries_ * select_, streamOf(res))
        , best_rows(num_queries_ * select_, streamOf(res))
    {
        const auto stream_view = streamOf(res);
        const cudaStream_t stream = stream_view.get();
        rmm::device_uvector<float> queries(num_queries * dimensions, stream_view);
        rmm::device_uvector<float> device_rotation(dimensions * dimensions, stream_view);
        checkCuda(cudaMemcpyAsync(queries.data(), host_queries, queries.size() * sizeof(float), cudaMemcpyHostToDevice, stream),
            "copying queries to the device");
        checkCuda(cudaMemcpyAsync(device_rotation.data(), rotation, device_rotation.size() * sizeof(float), cudaMemcpyHostToDevice, stream),
            "copying the rotation to the device");
        rotateRows(res, queries.data(), num_queries, device_rotation.data(), dimensions, rotated_queries.data());
        fillMissing(res, best_distances.data(), best_rows.data(), best_rows.size());
        raft::resource::sync_stream(res);
    }
};

__host__ size_t IvfListScan::maxSelect()
{
    return MAX_SELECT;
}

__host__ IvfListScan::IvfListScan(const float * queries, size_t num_queries, size_t dimensions, size_t select, const float * rotation)
{
    if (select == 0 || select > maxSelect())
        throwGPUError("the number of the nearest rows of an IVF search is out of range");
    translateErrors("starting an IVF search", [&] { state = new State(queries, num_queries, dimensions, select, rotation); });
}

__host__ IvfListScan::~IvfListScan()
{
    delete state;
}

__host__ void IvfListScan::scan(const char * chunk, size_t chunk_bytes, const uint64_t * list_begins, const uint32_t * list_sizes,
    size_t num_lists, const float * rotated_centroids, const uint32_t * query_list_offsets, const uint32_t * query_lists)
{
    translateErrors("scanning IVF lists", [&]
    {
        auto & res = state->res;
        const auto stream_view = streamOf(res);
        const cudaStream_t stream = stream_view.get();
        const size_t nq = state->num_queries;
        const size_t select = state->select;
        const size_t pairs = query_list_offsets[nq];
        if (pairs == 0)
            return;

        size_t max_lists_of_query = 0;
        for (size_t q = 0; q < nq; ++q)
            max_lists_of_query = std::max<size_t>(max_lists_of_query, query_list_offsets[q + 1] - query_list_offsets[q]);
        const size_t partial_width = select * (1 + max_lists_of_query);
        const bool per_list = nq * partial_width * (sizeof(float) + sizeof(uint32_t)) <= PARTIAL_RESULTS_BYTES;

        std::vector<uint32_t> block_query;
        std::vector<uint32_t> block_first;
        std::vector<uint32_t> block_last;
        std::vector<uint64_t> out_offsets;
        for (size_t q = 0; q < nq; ++q)
        {
            if (per_list)
            {
                for (uint32_t p = query_list_offsets[q]; p < query_list_offsets[q + 1]; ++p)
                {
                    block_query.push_back(uint32_t(q));
                    block_first.push_back(p);
                    block_last.push_back(p + 1);
                    out_offsets.push_back(q * partial_width + (1 + p - query_list_offsets[q]) * select);
                }
            }
            else
            {
                block_query.push_back(uint32_t(q));
                block_first.push_back(query_list_offsets[q]);
                block_last.push_back(query_list_offsets[q + 1]);
                out_offsets.push_back(q * select);
            }
        }
        const size_t blocks = block_query.size();

        rmm::device_uvector<char> device_chunk(chunk_bytes, stream_view);
        rmm::device_uvector<uint64_t> device_begins(num_lists, stream_view);
        rmm::device_uvector<uint32_t> device_sizes(num_lists, stream_view);
        rmm::device_uvector<float> device_rotated_centroids(num_lists * state->dimensions, stream_view);
        rmm::device_uvector<uint32_t> device_lists(pairs, stream_view);
        rmm::device_uvector<uint32_t> device_block_query(blocks, stream_view);
        rmm::device_uvector<uint32_t> device_block_first(blocks, stream_view);
        rmm::device_uvector<uint32_t> device_block_last(blocks, stream_view);
        rmm::device_uvector<uint64_t> device_out_offsets(blocks, stream_view);
        auto upload = [&](void * to, const void * from, size_t bytes)
        {
            checkCuda(cudaMemcpyAsync(to, from, bytes, cudaMemcpyHostToDevice, stream), "copying IVF lists to the device");
        };
        upload(device_chunk.data(), chunk, chunk_bytes);
        upload(device_begins.data(), list_begins, num_lists * sizeof(uint64_t));
        upload(device_sizes.data(), list_sizes, num_lists * sizeof(uint32_t));
        upload(device_rotated_centroids.data(), rotated_centroids, device_rotated_centroids.size() * sizeof(float));
        upload(device_lists.data(), query_lists, pairs * sizeof(uint32_t));
        upload(device_block_query.data(), block_query.data(), blocks * sizeof(uint32_t));
        upload(device_block_first.data(), block_first.data(), blocks * sizeof(uint32_t));
        upload(device_block_last.data(), block_last.data(), blocks * sizeof(uint32_t));
        upload(device_out_offsets.data(), out_offsets.data(), blocks * sizeof(uint64_t));

        auto scan_into = [&](const float * in_distances, const uint32_t * in_rows, float * out_distances, uint32_t * out_rows)
        {
            launchScan<32>(blocks, state->dimensions, select, stream, device_chunk.data(), device_begins.data(), device_sizes.data(),
                device_rotated_centroids.data(), device_block_query.data(), device_block_first.data(), device_block_last.data(),
                device_lists.data(), state->rotated_queries.data(), in_distances, in_rows, device_out_offsets.data(), out_distances, out_rows);
        };

        if (!per_list)
        {
            scan_into(state->best_distances.data(), state->best_rows.data(), state->best_distances.data(), state->best_rows.data());
            raft::resource::sync_stream(res);
            return;
        }

        /// A row per query: the best so far, then the best of each of its lists, merged by `select_k`.
        rmm::device_uvector<float> partial_distances(nq * partial_width, stream_view);
        rmm::device_uvector<uint32_t> partial_rows(nq * partial_width, stream_view);
        fillMissing(res, partial_distances.data(), partial_rows.data(), partial_rows.size());
        checkCuda(cudaMemcpy2DAsync(partial_distances.data(), partial_width * sizeof(float), state->best_distances.data(),
            select * sizeof(float), select * sizeof(float), nq, cudaMemcpyDeviceToDevice, stream), "copying the nearest rows");
        checkCuda(cudaMemcpy2DAsync(partial_rows.data(), partial_width * sizeof(uint32_t), state->best_rows.data(),
            select * sizeof(uint32_t), select * sizeof(uint32_t), nq, cudaMemcpyDeviceToDevice, stream), "copying the nearest rows");
        scan_into(nullptr, nullptr, partial_distances.data(), partial_rows.data());
        cuvs::selection::select_k(res,
            ConstRowMatrix(partial_distances.data(), int64_t(nq), int64_t(partial_width)),
            std::make_optional(raft::make_device_matrix_view<const uint32_t, int64_t, raft::row_major>(
                partial_rows.data(), int64_t(nq), int64_t(partial_width))),
            RowMatrix(state->best_distances.data(), int64_t(nq), int64_t(select)),
            raft::make_device_matrix_view<uint32_t, int64_t>(state->best_rows.data(), int64_t(nq), int64_t(select)),
            /*select_min=*/ true, /*sorted=*/ true);
        raft::resource::sync_stream(res);
    });
}

__host__ void IvfListScan::result(uint32_t * rows, float * distances) const
{
    translateErrors("copying the result of an IVF search", [&]
    {
        const cudaStream_t stream = streamOf(state->res).get();
        checkCuda(cudaMemcpyAsync(rows, state->best_rows.data(), state->best_rows.size() * sizeof(uint32_t), cudaMemcpyDeviceToHost, stream),
            "copying the nearest rows to the host");
        checkCuda(cudaMemcpyAsync(distances, state->best_distances.data(), state->best_distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
            stream), "copying the distances of the nearest rows to the host");
        raft::resource::sync_stream(state->res);
    });
}

}
