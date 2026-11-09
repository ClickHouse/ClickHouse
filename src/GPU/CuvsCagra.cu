#include <GPU/CuvsCagra.cuh>

#include <cuvs/neighbors/cagra.hpp>
#include <cuvs/neighbors/common.hpp>
#include <raft/core/device_mdarray.hpp>
#include <raft/core/error.hpp>
#include <raft/core/host_mdspan.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resources.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/error.hpp>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>

namespace DB::GPU
{

namespace
{

cuvs::distance::DistanceType cuvsDistanceOf(CagraDistance distance)
{
    return distance == CagraDistance::L2 ? cuvs::distance::DistanceType::L2Expanded : cuvs::distance::DistanceType::InnerProduct;
}

/// The resources of the calling thread, on its per-thread default stream: calls from different threads do not queue
/// behind each other, and cuVS keeps what it caches between searches (the descriptors of datasets) in them.
raft::resources & threadResources()
{
    thread_local raft::resources resources = []
    {
        raft::resources res;
        raft::resource::set_cuda_stream(res, rmm::cuda_stream_per_thread);
        return res;
    }();
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

        const auto stream = raft::resource::get_cuda_stream(res);
        checkCuda(cudaMemcpyAsync(graph, device_graph.data_handle(), rows * graph_degree * sizeof(uint32_t), cudaMemcpyDeviceToHost, stream),
            "copying a CAGRA graph to the host");
        raft::resource::sync_stream(res);
    });
}

struct CagraIndex::State
{
    std::unique_ptr<cuvs::neighbors::device_padded_dataset<float, int64_t>> dataset;
    std::optional<cuvs::neighbors::cagra::device_padded_index<float, uint32_t>> index;
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

        /// The graph is copied to the device by the index, which owns that copy.
        auto host_graph = raft::make_host_matrix_view<const uint32_t, int64_t>(
            graph, static_cast<int64_t>(rows), static_cast<int64_t>(graph_degree));
        new_state->index.emplace(res, cuvsDistanceOf(distance), new_state->dataset->as_dataset_view(), host_graph);
        raft::resource::sync_stream(res);

        new_state->device_memory_bytes = rows * (new_state->dataset->stride() * sizeof(float) + graph_degree * sizeof(uint32_t));
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
        const auto stream = raft::resource::get_cuda_stream(res);
        const auto rows = static_cast<int64_t>(num_queries);
        const auto k = static_cast<int64_t>(limit);

        auto device_queries = raft::make_device_matrix<float, int64_t>(res, rows, static_cast<int64_t>(state->dimensions));
        auto device_neighbours = raft::make_device_matrix<uint32_t, int64_t>(res, rows, k);
        auto device_distances = raft::make_device_matrix<float, int64_t>(res, rows, k);

        checkCuda(cudaMemcpyAsync(device_queries.data_handle(), queries, num_queries * state->dimensions * sizeof(float),
            cudaMemcpyHostToDevice, stream), "copying queries to the device");

        cuvs::neighbors::cagra::search_params params;
        params.itopk_size = std::max(candidates, limit);
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
