#include <GPU/CuvsIvf.cuh>
#include <GPU/GPUTypes.cuh>

#include <cuvs/cluster/kmeans.hpp>
#include <cuvs/selection/select_k.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/resource/cublas_handle.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resource/device_memory_resource.hpp>
#include <raft/core/resources.hpp>
#include <rmm/device_uvector.hpp>
#include <rmm/error.hpp>

#include <cublas_v2.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <mutex>
#include <string>
#include <vector>

namespace DB::GPU
{

namespace
{

constexpr int ARGMIN_THREADS = 256;

/// The scores of a chunk take at most this much device memory per matrix.
constexpr size_t SCORES_BYTES = 64 << 20;

/// The temporary memory of cuVS and raft. Their default is a quarter of the device, and the algorithms size their batches to
/// whatever they are given, so a build would take gigabytes.
constexpr size_t WORKSPACE_BYTES = 256 << 20;

/// k-means is trained on this many vectors per centroid at most, as FAISS advises at least 39: the sample is on the device.
constexpr size_t KMEANS_ROWS_PER_CENTROID = 40;

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

void checkCublas(cublasStatus_t status, const char * message)
{
    if (status != CUBLAS_STATUS_SUCCESS)
        throwGPUError((std::string(message) + ": cuBLAS status " + std::to_string(static_cast<int>(status))).c_str());
}

/// Block-wide argmin of (score, position), the smaller position on ties, as the CPU kernel takes the first best.
__device__ void blockArgmin(float & score, uint32_t & position)
{
    __shared__ float scores[ARGMIN_THREADS];
    __shared__ uint32_t positions[ARGMIN_THREADS];
    scores[threadIdx.x] = score;
    positions[threadIdx.x] = position;
    __syncthreads();
    for (int step = ARGMIN_THREADS / 2; step > 0; step /= 2)
    {
        if (threadIdx.x < step)
        {
            const float other = scores[threadIdx.x + step];
            const uint32_t other_position = positions[threadIdx.x + step];
            if (other < scores[threadIdx.x] || (other == scores[threadIdx.x] && other_position < positions[threadIdx.x]))
            {
                scores[threadIdx.x] = other;
                positions[threadIdx.x] = other_position;
            }
        }
        __syncthreads();
    }
    score = scores[0];
    position = positions[0];
}

/// One block per row: the nearest centroid by `|c|^2 - 2 <x, c>`; `dots` is row-major, `rows x num_centroids`.
__global__ void argminPrimary(const float * dots, const float * sq_norms, size_t num_centroids, uint32_t * primary)
{
    const float * row_dots = dots + blockIdx.x * num_centroids;
    float best = INFINITY;
    uint32_t best_position = UINT32_MAX;
    for (uint32_t c = threadIdx.x; c < num_centroids; c += ARGMIN_THREADS)
    {
        const float score = sq_norms[c] - 2.0f * row_dots[c];
        if (score < best)
        {
            best = score;
            best_position = c;
        }
    }
    blockArgmin(best, best_position);
    if (threadIdx.x == 0)
        primary[blockIdx.x] = best_position;
}

}

namespace
{

cuda::stream_ref streamOf(raft::resources & res)
{
    return raft::resource::get_cuda_stream(res);
}

}

struct IvfCentroids::State
{
    size_t num_centroids;
    size_t dimensions;
    mutable std::mutex mutex;
    mutable raft::resources resources;
    rmm::device_uvector<float> centroids;
    rmm::device_uvector<float> sq_norms;

    static raft::resources & limited(raft::resources & res)
    {
        limitWorkspace(res);
        return res;
    }

    State(const float * host_centroids, size_t num_centroids_, size_t dimensions_)
        : num_centroids(num_centroids_)
        , dimensions(dimensions_)
        , centroids(num_centroids_ * dimensions_, streamOf(limited(resources)))
        , sq_norms(num_centroids_, streamOf(resources))
    {
        std::vector<float> host_sq_norms(num_centroids);
        for (size_t c = 0; c < num_centroids; ++c)
        {
            double sum = 0;
            for (size_t i = 0; i < dimensions; ++i)
                sum += double(host_centroids[c * dimensions + i]) * host_centroids[c * dimensions + i];
            host_sq_norms[c] = float(sum);
        }
        const cudaStream_t stream = streamOf(resources).get();
        checkCuda(cudaMemcpyAsync(centroids.data(), host_centroids, centroids.size() * sizeof(float), cudaMemcpyHostToDevice, stream),
            "copying IVF centroids to the device");
        checkCuda(cudaMemcpyAsync(sq_norms.data(), host_sq_norms.data(), sq_norms.size() * sizeof(float), cudaMemcpyHostToDevice, stream),
            "copying IVF centroid norms to the device");
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
    return (state->centroids.size() + state->sq_norms.size()) * sizeof(float);
}

__host__ void IvfCentroids::assign(const float * vectors, size_t rows, uint32_t * positions) const
{
    translateErrors("assigning vectors to IVF centroids", [&]
    {
        std::lock_guard lock(state->mutex);
        auto & res = state->resources;
        const auto stream_view = streamOf(res);
        const cudaStream_t stream = stream_view.get();
        cublasHandle_t blas = raft::resource::get_cublas_handle(res);
        checkCublas(cublasSetStream(blas, stream), "setting the stream of cuBLAS");

        const size_t k = state->num_centroids;
        const size_t d = state->dimensions;
        const size_t chunk = std::clamp<size_t>(SCORES_BYTES / (k * sizeof(float)), 1, rows);

        rmm::device_uvector<float> device_vectors(chunk * d, stream_view);
        rmm::device_uvector<float> dots(chunk * k, stream_view);
        rmm::device_uvector<uint32_t> device_primary(chunk, stream_view);

        const float one = 1;
        const float zero = 0;
        for (size_t begin = 0; begin < rows; begin += chunk)
        {
            const size_t n = std::min(chunk, rows - begin);
            checkCuda(cudaMemcpyAsync(device_vectors.data(), vectors + begin * d, n * d * sizeof(float), cudaMemcpyHostToDevice, stream),
                "copying vectors to the device");
            /// Column-major: dots (k x n) = centroids^T (k x d) * vectors (d x n), which is row-major `n x k`.
            checkCublas(cublasSgemm(blas, CUBLAS_OP_T, CUBLAS_OP_N, int(k), int(n), int(d), &one, state->centroids.data(), int(d),
                device_vectors.data(), int(d), &zero, dots.data(), int(k)), "scoring vectors against centroids");
            argminPrimary<<<n, ARGMIN_THREADS, 0, stream>>>(dots.data(), state->sq_norms.data(), k, device_primary.data());
            checkCuda(cudaGetLastError(), "launching the assignment");
            checkCuda(cudaMemcpyAsync(positions + begin, device_primary.data(), n * sizeof(uint32_t), cudaMemcpyDeviceToHost, stream),
                "copying the assignment to the host");
            /// The next chunk overwrites the device buffers, and the host results must be complete on return.
            raft::resource::sync_stream(res);
        }
    });
}


namespace
{

constexpr uint32_t MISSING_ROW = std::numeric_limits<uint32_t>::max();

constexpr int SCAN_THREADS = 256;

/// The shared memory of a block of `scanChunk`: the best entries and the new candidates, sorted together.
constexpr size_t SCAN_SHARED_ENTRIES = 4096;

/// A small batch has a block per probed list of a query rather than per query, so that its lists are scanned on all of the
/// device; the best of each block go to a row of partial results per query, merged by `select_k`. Up to this many bytes.
constexpr size_t PARTIAL_RESULTS_BYTES = 256 << 20;

/// Per thread: the per-thread default stream keeps searching threads from queueing behind each other.
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

/// `scores[q][l] = |c_l|^2 - 2 dots[q][l]`, the order of the lists by L2 distance to the query.
__global__ void coarseScores(float * scores, const float * sq_norms, size_t num_lists, size_t total)
{
    const size_t i = blockIdx.x * size_t(blockDim.x) + threadIdx.x;
    if (i < total)
        scores[i] = sq_norms[i % num_lists] - 2.0f * scores[i];
}

__global__ void fillMissing(float * distances, uint32_t * rows, size_t total)
{
    const size_t i = blockIdx.x * size_t(blockDim.x) + threadIdx.x;
    if (i < total)
    {
        distances[i] = INFINITY;
        rows[i] = MISSING_ROW;
    }
}

/// Sorts `size` (a power of 2) pairs in shared memory by distance, bitonically.
__device__ void sortPairs(float * distances, uint32_t * rows, size_t size)
{
    for (size_t k = 2; k <= size; k <<= 1)
    {
        for (size_t j = k >> 1; j > 0; j >>= 1)
        {
            for (size_t i = threadIdx.x; i < size; i += blockDim.x)
            {
                const size_t other = i ^ j;
                if (other > i)
                {
                    const bool ascending = (i & k) == 0;
                    if ((distances[i] > distances[other]) == ascending)
                    {
                        const float d = distances[i];
                        distances[i] = distances[other];
                        distances[other] = d;
                        const uint32_t r = rows[i];
                        rows[i] = rows[other];
                        rows[other] = r;
                    }
                }
            }
            __syncthreads();
        }
    }
}

/// Block `b` scans the lists `query_lists[block_first[b] .. block_last[b])` of the chunk for the query `block_query[b]`, and
/// writes the best `select` entries to `out + out_offsets[b]`, merged with `in + block_query[b] * select` if `in` is set.
///
/// Shared memory holds `size` pairs: the best entries in `[0, select)`, sorted, and the new candidates after them. A candidate
/// better than the worst of the best entries is appended; when the space for candidates may not take another round of the
/// block, all are sorted together and the best `select` are kept.
__global__ void scanChunk(const char * chunk, const uint64_t * list_begins, const uint32_t * list_sizes,
    const uint32_t * block_query, const uint32_t * block_first, const uint32_t * block_last, const uint32_t * query_lists,
    const float * queries, size_t dimensions, bool cosine, size_t select, size_t size,
    const float * in_distances, const uint32_t * in_rows, const uint64_t * out_offsets, float * out_distances, uint32_t * out_rows)
{
    extern __shared__ char shared[];
    float * distances = reinterpret_cast<float *>(shared);
    uint32_t * rows = reinterpret_cast<uint32_t *>(distances + size);
    float * query = reinterpret_cast<float *>(rows + size);
    __shared__ unsigned count;
    __shared__ float threshold;

    const size_t q = block_query[blockIdx.x];
    const uint32_t first = block_first[blockIdx.x];
    const uint32_t last = block_last[blockIdx.x];
    if (first == last)
        return;

    for (size_t i = threadIdx.x; i < dimensions; i += blockDim.x)
        query[i] = queries[q * dimensions + i];
    for (size_t i = threadIdx.x; i < size; i += blockDim.x)
    {
        distances[i] = i < select && in_distances ? in_distances[q * select + i] : INFINITY;
        rows[i] = i < select && in_rows ? in_rows[q * select + i] : MISSING_ROW;
    }
    if (threadIdx.x == 0)
    {
        count = 0;
        threshold = distances[select - 1];
    }
    __syncthreads();

    const size_t room = size - select;
    auto merge = [&]
    {
        if (count == 0)
            return;
        for (size_t i = select + count + threadIdx.x; i < size; i += blockDim.x)
        {
            distances[i] = INFINITY;
            rows[i] = MISSING_ROW;
        }
        __syncthreads();
        sortPairs(distances, rows, size);
        if (threadIdx.x == 0)
        {
            count = 0;
            threshold = distances[select - 1];
        }
        __syncthreads();
    };

    for (uint32_t p = first; p < last; ++p)
    {
        const uint32_t list = query_lists[p];
        const char * begin = chunk + list_begins[list];
        const uint32_t entries = list_sizes[list];
        const uint32_t * entry_rows = reinterpret_cast<const uint32_t *>(begin);
        const float * entry_vectors = reinterpret_cast<const float *>(begin + size_t(entries) * sizeof(uint32_t));
        for (uint32_t start = 0; start < entries; start += blockDim.x)
        {
            /// `count` is the same for all threads here: it was last written before a barrier.
            if (count + blockDim.x > room)
                merge();

            const uint32_t i = start + threadIdx.x;
            if (i < entries)
            {
                const float * x = entry_vectors + size_t(i) * dimensions;
                float distance = 0;
                if (cosine)
                {
                    for (size_t j = 0; j < dimensions; ++j)
                        distance += x[j] * query[j];
                    distance = 1.0f - distance;
                }
                else
                {
                    for (size_t j = 0; j < dimensions; ++j)
                    {
                        const float diff = x[j] - query[j];
                        distance += diff * diff;
                    }
                }
                if (distance < threshold)
                {
                    const unsigned slot = atomicAdd(&count, 1u);
                    distances[select + slot] = distance;
                    rows[select + slot] = entry_rows[i];
                }
            }
            __syncthreads();
        }
    }
    merge();

    const uint64_t out = out_offsets[blockIdx.x];
    for (size_t i = threadIdx.x; i < select; i += blockDim.x)
    {
        out_distances[out + i] = distances[i];
        out_rows[out + i] = rows[i];
    }
}

/// `partial[q][0 .. select)` = `best[q]`, the first slot of a row of partial results.
__global__ void copyBestToPartial(const float * best_distances, const uint32_t * best_rows, size_t select, size_t width,
    size_t total, float * partial_distances, uint32_t * partial_rows)
{
    const size_t i = blockIdx.x * size_t(blockDim.x) + threadIdx.x;
    if (i < total)
    {
        const size_t q = i / select;
        partial_distances[q * width + i % select] = best_distances[i];
        partial_rows[q * width + i % select] = best_rows[i];
    }
}

size_t scanSharedEntries(size_t select)
{
    size_t size = 1;
    while (size < select + SCAN_THREADS)
        size <<= 1;
    return size;
}

}

__host__ void trainIvfCentroids(const float * vectors, size_t rows, size_t dimensions, size_t num_centroids, float * centroids)
{
    translateErrors("training IVF centroids", [&]
    {
        auto & res = searchResources();
        const auto stream_view = streamOf(res);
        const cudaStream_t stream = stream_view.get();

        /// Rows spread evenly over the vectors.
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
        cuvs::cluster::kmeans::fit(res, params,
            raft::make_device_matrix_view<const float, int64_t>(device_sample.data(), int64_t(sample), int64_t(dimensions)),
            raft::make_device_matrix_view<float, int64_t>(device_centroids.data(), int64_t(num_centroids), int64_t(dimensions)));

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
        cublasHandle_t blas = raft::resource::get_cublas_handle(res);
        checkCublas(cublasSetStream(blas, stream), "setting the stream of cuBLAS");

        const size_t k = state->num_centroids;
        const size_t d = state->dimensions;
        if (probes == 0 || probes > k)
            throwGPUError("the number of probed IVF lists must be in [1, the number of lists]");
        const size_t chunk = std::clamp<size_t>(SCORES_BYTES / (k * sizeof(float)), 1, num_queries);

        rmm::device_uvector<float> device_queries(chunk * d, stream_view);
        rmm::device_uvector<float> scores(chunk * k, stream_view);
        rmm::device_uvector<float> probe_scores(chunk * probes, stream_view);
        rmm::device_uvector<uint32_t> probe_lists(chunk * probes, stream_view);

        const float one = 1;
        const float zero = 0;
        for (size_t begin = 0; begin < num_queries; begin += chunk)
        {
            const size_t n = std::min(chunk, num_queries - begin);
            checkCuda(cudaMemcpyAsync(device_queries.data(), queries + begin * d, n * d * sizeof(float), cudaMemcpyHostToDevice, stream),
                "copying queries to the device");
            /// Column-major: dots (k x n) = centroids^T (k x d) * queries (d x n), which is row-major `n x k`.
            checkCublas(cublasSgemm(blas, CUBLAS_OP_T, CUBLAS_OP_N, int(k), int(n), int(d), &one, state->centroids.data(), int(d),
                device_queries.data(), int(d), &zero, scores.data(), int(k)), "scoring queries against centroids");
            coarseScores<<<(n * k + 255) / 256, 256, 0, stream>>>(scores.data(), state->sq_norms.data(), k, n * k);
            checkCuda(cudaGetLastError(), "launching the scores of the lists");
            cuvs::selection::select_k(res,
                raft::make_device_matrix_view<const float, int64_t>(scores.data(), int64_t(n), int64_t(k)),
                std::optional<raft::device_matrix_view<const uint32_t, int64_t, raft::row_major>>{},
                raft::make_device_matrix_view<float, int64_t>(probe_scores.data(), int64_t(n), int64_t(probes)),
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
    size_t shared_entries;
    IvfDistance distance;
    raft::resources & res;
    rmm::device_uvector<float> queries;
    rmm::device_uvector<float> best_distances;
    rmm::device_uvector<uint32_t> best_rows;

    State(const float * host_queries, size_t num_queries_, size_t dimensions_, size_t select_, IvfDistance distance_)
        : num_queries(num_queries_)
        , dimensions(dimensions_)
        , select(select_)
        , shared_entries(scanSharedEntries(select_))
        , distance(distance_)
        , res(searchResources())
        , queries(num_queries_ * dimensions_, streamOf(res))
        , best_distances(num_queries_ * select_, streamOf(res))
        , best_rows(num_queries_ * select_, streamOf(res))
    {
        const cudaStream_t stream = streamOf(res).get();
        checkCuda(cudaMemcpyAsync(queries.data(), host_queries, queries.size() * sizeof(float), cudaMemcpyHostToDevice, stream),
            "copying queries to the device");
        fillMissing<<<(best_rows.size() + 255) / 256, 256, 0, stream>>>(best_distances.data(), best_rows.data(), best_rows.size());
        checkCuda(cudaGetLastError(), "launching the initialization of the nearest rows");
        raft::resource::sync_stream(res);
    }
};

__host__ size_t IvfListScan::maxSelect()
{
    return SCAN_SHARED_ENTRIES - SCAN_THREADS;
}

__host__ IvfListScan::IvfListScan(const float * queries, size_t num_queries, size_t dimensions, size_t select, IvfDistance distance)
{
    if (select == 0 || select > maxSelect())
        throwGPUError("the number of the nearest rows of an IVF search is out of range");
    translateErrors("starting an IVF search", [&] { state = new State(queries, num_queries, dimensions, select, distance); });
}

__host__ IvfListScan::~IvfListScan()
{
    delete state;
}

__host__ void IvfListScan::scan(const char * chunk, size_t chunk_bytes, const uint64_t * list_begins, const uint32_t * list_sizes,
    size_t num_lists, const uint32_t * query_list_offsets, const uint32_t * query_lists)
{
    translateErrors("scanning IVF lists", [&]
    {
        auto & res = state->res;
        const auto stream_view = streamOf(res);
        const cudaStream_t stream = stream_view.get();
        const size_t nq = state->num_queries;
        const size_t pairs = query_list_offsets[nq];
        if (pairs == 0)
            return;

        /// A block per query, or, for a small batch, per probed list of a query.
        size_t max_lists_of_query = 0;
        for (size_t q = 0; q < nq; ++q)
            max_lists_of_query = std::max<size_t>(max_lists_of_query, query_list_offsets[q + 1] - query_list_offsets[q]);
        const size_t partial_width = state->select * (1 + max_lists_of_query);
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
                    out_offsets.push_back(q * partial_width + (1 + p - query_list_offsets[q]) * state->select);
                }
            }
            else
            {
                block_query.push_back(uint32_t(q));
                block_first.push_back(query_list_offsets[q]);
                block_last.push_back(query_list_offsets[q + 1]);
                out_offsets.push_back(q * state->select);
            }
        }
        const size_t blocks = block_query.size();

        rmm::device_uvector<char> device_chunk(chunk_bytes, stream_view);
        rmm::device_uvector<uint64_t> device_begins(num_lists, stream_view);
        rmm::device_uvector<uint32_t> device_sizes(num_lists, stream_view);
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
        upload(device_lists.data(), query_lists, pairs * sizeof(uint32_t));
        upload(device_block_query.data(), block_query.data(), blocks * sizeof(uint32_t));
        upload(device_block_first.data(), block_first.data(), blocks * sizeof(uint32_t));
        upload(device_block_last.data(), block_last.data(), blocks * sizeof(uint32_t));
        upload(device_out_offsets.data(), out_offsets.data(), blocks * sizeof(uint64_t));

        const size_t shared_bytes = state->shared_entries * (sizeof(float) + sizeof(uint32_t)) + state->dimensions * sizeof(float);
        /// Beyond the default 48 KiB, as for large dimensions.
        if (shared_bytes > 48 * 1024)
            checkCuda(cudaFuncSetAttribute(scanChunk, cudaFuncAttributeMaxDynamicSharedMemorySize, int(shared_bytes)),
                "allowing the shared memory of the scan of IVF lists");

        const bool cosine = state->distance == IvfDistance::Cosine;
        if (per_list)
        {
            rmm::device_uvector<float> partial_distances(nq * partial_width, stream_view);
            rmm::device_uvector<uint32_t> partial_rows(nq * partial_width, stream_view);
            fillMissing<<<(partial_rows.size() + 255) / 256, 256, 0, stream>>>(partial_distances.data(), partial_rows.data(), partial_rows.size());
            copyBestToPartial<<<(nq * state->select + 255) / 256, 256, 0, stream>>>(state->best_distances.data(), state->best_rows.data(),
                state->select, partial_width, nq * state->select, partial_distances.data(), partial_rows.data());
            scanChunk<<<blocks, SCAN_THREADS, shared_bytes, stream>>>(device_chunk.data(), device_begins.data(), device_sizes.data(),
                device_block_query.data(), device_block_first.data(), device_block_last.data(), device_lists.data(), state->queries.data(),
                state->dimensions, cosine, state->select, state->shared_entries, nullptr, nullptr, device_out_offsets.data(),
                partial_distances.data(), partial_rows.data());
            checkCuda(cudaGetLastError(), "launching the scan of IVF lists");
            cuvs::selection::select_k(res,
                raft::make_device_matrix_view<const float, int64_t>(partial_distances.data(), int64_t(nq), int64_t(partial_width)),
                std::make_optional(raft::make_device_matrix_view<const uint32_t, int64_t>(partial_rows.data(), int64_t(nq), int64_t(partial_width))),
                raft::make_device_matrix_view<float, int64_t>(state->best_distances.data(), int64_t(nq), int64_t(state->select)),
                raft::make_device_matrix_view<uint32_t, int64_t>(state->best_rows.data(), int64_t(nq), int64_t(state->select)),
                /*select_min=*/ true, /*sorted=*/ true);
            /// The partial results die on return.
            raft::resource::sync_stream(res);
            return;
        }

        scanChunk<<<blocks, SCAN_THREADS, shared_bytes, stream>>>(device_chunk.data(), device_begins.data(), device_sizes.data(),
            device_block_query.data(), device_block_first.data(), device_block_last.data(), device_lists.data(), state->queries.data(),
            state->dimensions, cosine, state->select, state->shared_entries, state->best_distances.data(), state->best_rows.data(),
            device_out_offsets.data(), state->best_distances.data(), state->best_rows.data());
        checkCuda(cudaGetLastError(), "launching the scan of IVF lists");
        /// The host buffers may be reused after the return.
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
