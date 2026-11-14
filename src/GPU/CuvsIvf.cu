#include <GPU/CuvsIvf.cuh>
#include <GPU/GPUTypes.cuh>
#include <GPU/IvfQuantization.cuh>

#include <cuvs/cluster/kmeans.hpp>
#include <cuvs/selection/select_k.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resource/device_memory_resource.hpp>
#include <raft/core/resources.hpp>
#include <raft/linalg/gemm.cuh>
#include <raft/linalg/matrix_vector_op.cuh>
#include <raft/linalg/norm.cuh>
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

constexpr size_t KMEANS_ROWS_PER_CENTROID = 40;

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

struct IvfCentroids::State
{
    size_t num_centroids;
    size_t dimensions;
    mutable raft::resources resources;
    rmm::device_uvector<float> centroids;
    rmm::device_uvector<float> centroid_squared_lengths;

    static raft::resources & limited(raft::resources & res)
    {
        limitWorkspace(res);
        return res;
    }

    State(const float * host_centroids, size_t num_centroids_, size_t dimensions_)
        : num_centroids(num_centroids_)
        , dimensions(dimensions_)
        , centroids(num_centroids_ * dimensions_, streamOf(limited(resources)))
        , centroid_squared_lengths(num_centroids_, streamOf(resources))
    {
        checkCuda(cudaMemcpyAsync(centroids.data(), host_centroids, centroids.size() * sizeof(float), cudaMemcpyHostToDevice,
            streamOf(resources).get()), "copying IVF centroids to the device");
        raft::linalg::norm<raft::linalg::L2Norm, raft::Apply::ALONG_ROWS>(resources,
            ConstRowMatrix(centroids.data(), int64_t(num_centroids), int64_t(dimensions)),
            raft::make_device_vector_view<float, int64_t>(centroid_squared_lengths.data(), int64_t(num_centroids)));
        raft::resource::sync_stream(resources);
    }
};

__host__ IvfCentroids::IvfCentroids(const float * centroids, size_t num_centroids, size_t dimensions)
{
    translateErrors("uploading IVF centroids", [&] { state = new State(centroids, num_centroids, dimensions); });
}

__host__ IvfCentroids::~IvfCentroids()
{
    delete state;
}

__host__ size_t IvfCentroids::deviceMemoryBytes() const
{
    return (state->centroids.size() + state->centroid_squared_lengths.size()) * sizeof(float);
}

__host__ void IvfCentroids::assign(const float * vectors, size_t rows, uint32_t * positions, const float * rotation, float * rotated) const
{
    translateErrors("assigning vectors to IVF centroids", [&]
    {
        auto & res = state->resources;
        const auto stream_view = streamOf(res);
        const cudaStream_t stream = stream_view.get();

        const size_t k = state->num_centroids;
        const size_t d = state->dimensions;
        const size_t chunk = std::clamp<size_t>(SCORES_BYTES / (d * sizeof(float)), 1, rows);

        rmm::device_uvector<float> device_vectors(chunk * d, stream_view);
        rmm::device_uvector<uint32_t> device_primary(chunk, stream_view);
        rmm::device_uvector<float> device_rotation(rotated ? d * d : 0, stream_view);
        rmm::device_uvector<float> device_rotated(rotated ? chunk * d : 0, stream_view);
        if (rotated)
            checkCuda(cudaMemcpyAsync(device_rotation.data(), rotation, d * d * sizeof(float), cudaMemcpyHostToDevice, stream),
                "copying the rotation to the device");

        const auto device_centroids = ConstRowMatrix(state->centroids.data(), int64_t(k), int64_t(d));
        const cuvs::cluster::kmeans::balanced_params params;
        for (size_t begin = 0; begin < rows; begin += chunk)
        {
            const size_t n = std::min(chunk, rows - begin);
            checkCuda(cudaMemcpyAsync(device_vectors.data(), vectors + begin * d, n * d * sizeof(float), cudaMemcpyHostToDevice, stream),
                "copying vectors to the device");
            cuvs::cluster::kmeans::predict(res, params, ConstRowMatrix(device_vectors.data(), int64_t(n), int64_t(d)), device_centroids,
                raft::make_device_vector_view<uint32_t, int64_t>(device_primary.data(), int64_t(n)));
            checkCuda(cudaMemcpyAsync(positions + begin, device_primary.data(), n * sizeof(uint32_t), cudaMemcpyDeviceToHost, stream),
                "copying the assignment to the host");
            if (rotated)
            {
                rotateRows(res, device_vectors.data(), n, device_rotation.data(), d, device_rotated.data());
                checkCuda(cudaMemcpyAsync(rotated + begin * d, device_rotated.data(), n * d * sizeof(float), cudaMemcpyDeviceToHost, stream),
                    "copying the rotated vectors to the host");
            }
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

__host__ void trainIvfCentroids(const float * vectors, size_t rows, size_t dimensions, size_t num_centroids, float * centroids)
{
    translateErrors("training IVF centroids", [&]
    {
        auto & res = searchResources();
        const auto stream_view = streamOf(res);
        const cudaStream_t stream = stream_view.get();

        const size_t sample = std::min(rows, std::max<size_t>(num_centroids * KMEANS_ROWS_PER_CENTROID, 65536));
        std::vector<float> host_sample(sample * dimensions);
        for (size_t i = 0; i < sample; ++i)
        {
            const size_t row = i * rows / sample;
            std::copy(vectors + row * dimensions, vectors + (row + 1) * dimensions, host_sample.data() + i * dimensions);
        }

        rmm::device_uvector<float> device_sample(host_sample.size(), stream_view);
        rmm::device_uvector<float> device_centroids(num_centroids * dimensions, stream_view);
        checkCuda(cudaMemcpyAsync(device_sample.data(), host_sample.data(), host_sample.size() * sizeof(float), cudaMemcpyHostToDevice, stream),
            "copying the k-means sample to the device");

        cuvs::cluster::kmeans::balanced_params params;
        cuvs::cluster::kmeans::fit(res, params, ConstRowMatrix(device_sample.data(), int64_t(sample), int64_t(dimensions)),
            RowMatrix(device_centroids.data(), int64_t(num_centroids), int64_t(dimensions)));

        checkCuda(cudaMemcpyAsync(centroids, device_centroids.data(), device_centroids.size() * sizeof(float), cudaMemcpyDeviceToHost, stream),
            "copying the IVF centroids to the host");
        raft::resource::sync_stream(res);
    });
}

__host__ void IvfCentroids::nearestLists(const float * queries, size_t num_queries, size_t probes, uint32_t * lists) const
{
    translateErrors("probing IVF centroids", [&]
    {
        auto & res = searchResources();
        const auto stream_view = streamOf(res);
        const cudaStream_t stream = stream_view.get();

        const size_t k = state->num_centroids;
        const size_t d = state->dimensions;
        if (probes == 0 || probes > k)
            throwGPUError("the number of probed IVF lists must be in [1, the number of lists]");
        const size_t chunk = std::clamp<size_t>(SCORES_BYTES / (k * sizeof(float)), 1, num_queries);

        rmm::device_uvector<float> device_queries(chunk * d, stream_view);
        rmm::device_uvector<float> scores(chunk * k, stream_view);
        rmm::device_uvector<float> probe_scores(chunk * probes, stream_view);
        rmm::device_uvector<uint32_t> probe_lists(chunk * probes, stream_view);
        float minus_two = -2;

        for (size_t begin = 0; begin < num_queries; begin += chunk)
        {
            const size_t n = std::min(chunk, num_queries - begin);
            checkCuda(cudaMemcpyAsync(device_queries.data(), queries + begin * d, n * d * sizeof(float), cudaMemcpyHostToDevice, stream),
                "copying queries to the device");
            const auto scores_view = RowMatrix(scores.data(), int64_t(n), int64_t(k));
            raft::linalg::gemm(res, RowMatrix(device_queries.data(), int64_t(n), int64_t(d)),
                raft::make_device_matrix_view<float, int64_t, raft::col_major>(state->centroids.data(), int64_t(d), int64_t(k)), scores_view,
                std::make_optional(raft::make_host_scalar_view<float, uint32_t>(&minus_two)));
            raft::linalg::matrix_vector_op<raft::Apply::ALONG_ROWS>(res, raft::make_const_mdspan(scores_view),
                raft::make_device_vector_view<const float, int64_t>(state->centroid_squared_lengths.data(), int64_t(k)), scores_view,
                raft::add_op{});
            cuvs::selection::select_k(res, raft::make_const_mdspan(scores_view),
                std::optional<raft::device_matrix_view<const uint32_t, int64_t, raft::row_major>>{},
                RowMatrix(probe_scores.data(), int64_t(n), int64_t(probes)),
                raft::make_device_matrix_view<uint32_t, int64_t>(probe_lists.data(), int64_t(n), int64_t(probes)),
                /*select_min=*/ true, /*sorted=*/ true);
            checkCuda(cudaMemcpyAsync(lists + begin * probes, probe_lists.data(), n * probes * sizeof(uint32_t), cudaMemcpyDeviceToHost, stream),
                "copying the probed lists to the host");
            raft::resource::sync_stream(res);
        }
    });
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
