#include <GPU/CuvsCagra.cuh>

#include <cuvs/neighbors/cagra.hpp>
#include <cuvs/neighbors/common.hpp>
#include <cuvs/preprocessing/quantize/pq.hpp>
#include <raft/core/device_mdarray.hpp>
#include <raft/core/error.hpp>
#include <raft/core/host_mdspan.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resources.hpp>
#include <rmm/error.hpp>

#include <cuda_fp16.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace DB::GPU
{

namespace
{

cuvs::distance::DistanceType cuvsDistanceOf(CagraDistance distance)
{
    return distance == CagraDistance::L2 ? cuvs::distance::DistanceType::L2Expanded : cuvs::distance::DistanceType::InnerProduct;
}

/// Per thread: the per-thread default stream keeps threads from queueing behind each other, and cuVS caches dataset
/// descriptors here.
raft::resources & threadResources()
{
    thread_local raft::resources resources;
    return resources;
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
    catch (const raft::cuda_error & exception)
    {
        throwGPUError((std::string(doing) + ": CUDA error: " + exception.what()).c_str());
    }
    catch (const raft::exception & exception)
    {
        throwGPUError((std::string(doing) + ": " + exception.what()).c_str());
    }
    catch (const std::exception & exception)
    {
        if (isClickHouseException(exception))
            throw;
        throwGPUError((std::string(doing) + ": " + exception.what()).c_str());
    }
}

}

__host__ void buildCagraGraph(
    const float * dataset, size_t rows, size_t dimensions, CagraDistance distance,
    size_t graph_degree, size_t intermediate_graph_degree, uint32_t * graph)
{
    translateErrors("building a CAGRA graph", [&]
    {
        auto & res = threadResources();

        cuvs::neighbors::cagra::index_params params;
        params.metric = cuvsDistanceOf(distance);
        params.graph_degree = graph_degree;
        params.intermediate_graph_degree = intermediate_graph_degree;

        auto host_dataset = raft::make_host_matrix_view<const float, int64_t>(
            dataset, static_cast<int64_t>(rows), static_cast<int64_t>(dimensions));
        auto index = cuvs::neighbors::cagra::build(res, params, cuvs::neighbors::make_host_standard_dataset_view(host_dataset));

        const auto device_graph = index.graph();
        if (static_cast<size_t>(device_graph.extent(0)) != rows || static_cast<size_t>(device_graph.extent(1)) != graph_degree)
            throwGPUError("CAGRA built a graph of unexpected shape");

        const cudaStream_t stream = raft::resource::get_cuda_stream(res).get();
        checkCuda(cudaMemcpyAsync(graph, device_graph.data_handle(), rows * graph_degree * sizeof(uint32_t), cudaMemcpyDeviceToHost, stream),
            "copying a CAGRA graph to the host");
        raft::resource::sync_stream(res);
    });
}

using DeviceVpqDataset = cuvs::neighbors::device_vpq_dataset<half, int64_t>;

namespace
{

/// Host floats to device halves.
raft::device_matrix<half, uint32_t, raft::row_major> uploadCodebook(raft::resources & res, const float * codebook, size_t rows, size_t columns)
{
    std::vector<half> halves(rows * columns);
    for (size_t i = 0; i < halves.size(); ++i)
        halves[i] = __float2half(codebook[i]);
    auto device_codebook = raft::make_device_matrix<half, uint32_t>(res, static_cast<uint32_t>(rows), static_cast<uint32_t>(columns));
    checkCuda(cudaMemcpyAsync(device_codebook.data_handle(), halves.data(), halves.size() * sizeof(half), cudaMemcpyHostToDevice,
        raft::resource::get_cuda_stream(res).get()), "copying a VPQ codebook to the device");
    /// `halves` dies on return.
    raft::resource::sync_stream(res);
    return device_codebook;
}

void downloadCodebook(raft::resources & res, const half * device_codebook, size_t size, float * codebook)
{
    std::vector<half> halves(size);
    checkCuda(cudaMemcpyAsync(halves.data(), device_codebook, size * sizeof(half), cudaMemcpyDeviceToHost,
        raft::resource::get_cuda_stream(res).get()), "copying a VPQ codebook to the host");
    raft::resource::sync_stream(res);
    for (size_t i = 0; i < size; ++i)
        codebook[i] = __half2float(halves[i]);
}

}

struct CagraCompressedDataset::State
{
    std::optional<DeviceVpqDataset> dataset;
};

__host__ CagraCompressedDataset::CagraCompressedDataset(const float * dataset, size_t rows, size_t dimensions, size_t pq_len)
{
    translateErrors("compressing vectors for CAGRA-Q", [&]
    {
        auto & res = threadResources();
        auto new_state = std::make_unique<State>();

        cuvs::neighbors::vpq_params params;
        params.pq_bits = 8;
        params.pq_dim = static_cast<uint32_t>(dimensions / pq_len);

        auto host_dataset = raft::make_host_matrix_view<const float, int64_t>(
            dataset, static_cast<int64_t>(rows), static_cast<int64_t>(dimensions));
        new_state->dataset.emplace(cuvs::preprocessing::quantize::pq::make_vpq_dataset(res, params, host_dataset));
        raft::resource::sync_stream(res);

        if (new_state->dataset->pq_len() != pq_len || new_state->dataset->pq_n_centers() != cagra_pq_codebook_rows
            || static_cast<size_t>(new_state->dataset->n_rows()) != rows)
            throwGPUError("VPQ compressed the vectors in an unexpected shape");
        state = new_state.release();
    });
}

__host__ CagraCompressedDataset::~CagraCompressedDataset()
{
    delete state;
}

__host__ size_t CagraCompressedDataset::vqCenters() const
{
    return state->dataset->vq_n_centers();
}

__host__ size_t CagraCompressedDataset::rowBytes() const
{
    return state->dataset->encoded_row_length();
}

__host__ void CagraCompressedDataset::copyToHost(float * vq_codebook, float * pq_codebook, uint8_t * codes) const
{
    translateErrors("copying vectors compressed for CAGRA-Q to the host", [&]
    {
        auto & res = threadResources();
        const auto & dataset = *state->dataset;
        downloadCodebook(res, dataset.vq_code_book.data_handle(), dataset.vq_code_book.size(), vq_codebook);
        downloadCodebook(res, dataset.pq_code_book.data_handle(), dataset.pq_code_book.size(), pq_codebook);
        checkCuda(cudaMemcpyAsync(codes, dataset.data.data_handle(), dataset.data.size(), cudaMemcpyDeviceToHost,
            raft::resource::get_cuda_stream(res).get()), "copying compressed vectors to the host");
        raft::resource::sync_stream(res);
    });
}

struct CagraIndex::State
{
    std::unique_ptr<cuvs::neighbors::device_padded_dataset<float, int64_t>> dataset;
    std::optional<cuvs::neighbors::cagra::device_padded_index<float, uint32_t>> index;
    /// CAGRA-Q, instead of the two above.
    std::optional<DeviceVpqDataset> compressed_dataset;
    std::optional<cuvs::neighbors::cagra::device_pq_index<float, uint32_t, half>> compressed_index;
    size_t dimensions = 0;
    size_t device_memory_bytes = 0;
};

__host__ CagraIndex::CagraIndex(const float * dataset, const uint32_t * graph, size_t rows, size_t dimensions, size_t graph_degree,
    CagraDistance distance)
{
    translateErrors("uploading a CAGRA index", [&]
    {
        auto & res = threadResources();
        auto new_state = std::make_unique<State>();
        new_state->dimensions = dimensions;

        auto host_dataset = raft::make_host_matrix_view<const float, int64_t>(
            dataset, static_cast<int64_t>(rows), static_cast<int64_t>(dimensions));
        new_state->dataset = cuvs::neighbors::make_device_padded_dataset(res, host_dataset);

        /// The index copies the graph to the device.
        auto host_graph = raft::make_host_matrix_view<const uint32_t, int64_t>(
            graph, static_cast<int64_t>(rows), static_cast<int64_t>(graph_degree));
        new_state->index.emplace(res, cuvsDistanceOf(distance), new_state->dataset->as_dataset_view(), host_graph);
        raft::resource::sync_stream(res);

        new_state->device_memory_bytes = rows * (new_state->dataset->stride() * sizeof(float) + graph_degree * sizeof(uint32_t));
        state = new_state.release();
    });
}

__host__ CagraIndex::CagraIndex(const float * vq_codebook, size_t vq_centers, const float * pq_codebook, size_t pq_len,
    const uint8_t * codes, size_t row_bytes, const uint32_t * graph, size_t rows, size_t dimensions, size_t graph_degree)
{
    translateErrors("uploading a CAGRA-Q index", [&]
    {
        auto & res = threadResources();
        auto new_state = std::make_unique<State>();
        new_state->dimensions = dimensions;

        auto device_codes = raft::make_device_matrix<uint8_t, int64_t>(res, static_cast<int64_t>(rows), static_cast<int64_t>(row_bytes));
        checkCuda(cudaMemcpyAsync(device_codes.data_handle(), codes, rows * row_bytes, cudaMemcpyHostToDevice,
            raft::resource::get_cuda_stream(res).get()), "copying compressed vectors to the device");
        new_state->compressed_dataset.emplace(
            uploadCodebook(res, vq_codebook, vq_centers, dimensions),
            uploadCodebook(res, pq_codebook, cagra_pq_codebook_rows, pq_len),
            std::move(device_codes));

        auto host_graph = raft::make_host_matrix_view<const uint32_t, int64_t>(
            graph, static_cast<int64_t>(rows), static_cast<int64_t>(graph_degree));
        new_state->compressed_index.emplace(res, cuvs::distance::DistanceType::L2Expanded,
            new_state->compressed_dataset->as_dataset_view(), host_graph);
        raft::resource::sync_stream(res);

        new_state->device_memory_bytes = rows * (row_bytes + graph_degree * sizeof(uint32_t))
            + (vq_centers * dimensions + cagra_pq_codebook_rows * pq_len) * sizeof(half);
        state = new_state.release();
    });
}

__host__ CagraIndex::~CagraIndex()
{
    delete state;
}

__host__ void CagraIndex::search(const float * queries, size_t num_queries, size_t limit, size_t candidates,
    uint32_t * neighbours, float * distances) const
{
    translateErrors("searching a CAGRA index", [&]
    {
        auto & res = threadResources();
        const cudaStream_t stream = raft::resource::get_cuda_stream(res).get();
        const auto rows = static_cast<int64_t>(num_queries);
        const auto k = static_cast<int64_t>(limit);

        auto device_queries = raft::make_device_matrix<float, int64_t>(res, rows, static_cast<int64_t>(state->dimensions));
        auto device_neighbours = raft::make_device_matrix<uint32_t, int64_t>(res, rows, k);
        auto device_distances = raft::make_device_matrix<float, int64_t>(res, rows, k);

        checkCuda(cudaMemcpyAsync(device_queries.data_handle(), queries, num_queries * state->dimensions * sizeof(float),
            cudaMemcpyHostToDevice, stream), "copying queries to the device");

        cuvs::neighbors::cagra::search_params params;
        params.itopk_size = std::max(candidates, limit);
        if (state->compressed_index)
            cuvs::neighbors::cagra::search(res, params, *state->compressed_index,
                raft::make_const_mdspan(device_queries.view()), device_neighbours.view(), device_distances.view());
        else
            cuvs::neighbors::cagra::search(res, params, *state->index,
                raft::make_const_mdspan(device_queries.view()), device_neighbours.view(), device_distances.view());

        checkCuda(cudaMemcpyAsync(neighbours, device_neighbours.data_handle(), num_queries * limit * sizeof(uint32_t),
            cudaMemcpyDeviceToHost, stream), "copying neighbours to the host");
        checkCuda(cudaMemcpyAsync(distances, device_distances.data_handle(), num_queries * limit * sizeof(float),
            cudaMemcpyDeviceToHost, stream), "copying distances to the host");
        raft::resource::sync_stream(res);
    });
}

__host__ size_t CagraIndex::deviceMemoryBytes() const
{
    return state->device_memory_bytes;
}

}
