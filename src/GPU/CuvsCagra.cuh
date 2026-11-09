#pragma once

#include <GPU/GPUTypes.cuh>

#include <cstddef>
#include <cstdint>

/// CAGRA of cuVS, for the `cagra` method of the `vector_similarity` index. On the island: cuVS, RAFT and RMM are of
/// libstdc++, so only plain pointers and sizes cross this header. The device memory comes from the RMM resource of the
/// `GPUDevice`, the one cuDF allocates from as well.
namespace DB::GPU
{

enum class CagraDistance : int
{
    /// The squared L2 distance.
    L2 = 0,
    /// The inner product; a larger one is nearer.
    InnerProduct = 1,
};

/// The CAGRA graph of `rows` vectors of `dimensions` floats in host memory, built on the device: `rows * graph_degree`
/// neighbours written to `graph`.
__host__ void buildCagraGraph(
    const float * dataset, size_t rows, size_t dimensions, CagraDistance distance,
    size_t graph_degree, size_t intermediate_graph_degree, uint32_t * graph);

/// A CAGRA index on the device, made from the vectors and the graph that `buildCagraGraph` built, both in host memory
/// and copied to the device. It may be searched by several threads at once.
class CagraIndex
{
public:
    __host__ CagraIndex(const float * dataset, const uint32_t * graph, size_t rows, size_t dimensions, size_t graph_degree,
        CagraDistance distance);
    __host__ ~CagraIndex();

    CagraIndex(const CagraIndex &) = delete;
    CagraIndex & operator=(const CagraIndex &) = delete;

    /// The `limit` nearest rows to each of `num_queries` queries of `dimensions` floats in host memory, with a list of
    /// `candidates` entries (the `itopk_size` of CAGRA, raised to `limit` if smaller). Writes `num_queries * limit` rows
    /// and distances to host memory; a row that was not found is `std::numeric_limits<uint32_t>::max()`.
    __host__ void search(const float * queries, size_t num_queries, size_t limit, size_t candidates,
        uint32_t * neighbours, float * distances) const;

    /// The device memory it holds.
    __host__ size_t deviceMemoryBytes() const;

private:
    struct State;
    State * state = nullptr;
};

}
