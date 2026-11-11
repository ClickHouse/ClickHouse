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

/// The number of rows of the PQ codebook of CAGRA-Q: its codes are of 8 bits, the only width its search supports.
constexpr size_t cagra_pq_codebook_rows = 256;

/// The CAGRA graph of `rows` vectors of `dimensions` floats in host memory, built on the device: `rows * graph_degree`
/// neighbours written to `graph`.
__host__ void buildCagraGraph(
    const float * dataset, size_t rows, size_t dimensions, CagraDistance distance,
    size_t graph_degree, size_t intermediate_graph_degree, uint32_t * graph);

/// `rows` vectors of `dimensions` floats in host memory compressed by VPQ, as CAGRA-Q searches them. A row is
/// approximated by the nearest of the VQ centers and its residual from that center, which is cut into pieces of `pq_len`
/// components, each approximated by the nearest row of the PQ codebook, shared by all pieces. A compressed row is the
/// number of its VQ center (`uint32_t`) and the numbers of the rows of the PQ codebook for its `dimensions / pq_len`
/// pieces, a byte each, padded to a multiple of four bytes.
///
/// The codebooks are trained on a sample of the rows, and the rows are encoded in batches, so the dataset is never
/// on the device as a whole.
class CagraCompressedDataset
{
public:
    __host__ CagraCompressedDataset(const float * dataset, size_t rows, size_t dimensions, size_t pq_len);
    __host__ ~CagraCompressedDataset();

    CagraCompressedDataset(const CagraCompressedDataset &) = delete;
    CagraCompressedDataset & operator=(const CagraCompressedDataset &) = delete;

    __host__ size_t vqCenters() const;
    __host__ size_t rowBytes() const;

    /// Copies it to host memory: `vqCenters() * dimensions` floats of the VQ codebook, `cagra_pq_codebook_rows * pq_len`
    /// floats of the PQ codebook and `rows * rowBytes()` bytes of the compressed rows.
    __host__ void copyToHost(float * vq_codebook, float * pq_codebook, uint8_t * codes) const;

private:
    struct State;
    State * state = nullptr;
};

/// A CAGRA index on the device, made from the vectors and the graph that `buildCagraGraph` built, both in host memory
/// and copied to the device. It may be searched by several threads at once.
class CagraIndex
{
public:
    __host__ CagraIndex(const float * dataset, const uint32_t * graph, size_t rows, size_t dimensions, size_t graph_degree,
        CagraDistance distance);

    /// The same over the vectors compressed by `CagraCompressedDataset`, as copied to the host by it (CAGRA-Q). It is
    /// searched by the squared L2 distance to the decompressed vectors, the only distance CAGRA-Q supports.
    __host__ CagraIndex(const float * vq_codebook, size_t vq_centers, const float * pq_codebook, size_t pq_len,
        const uint8_t * codes, size_t row_bytes, const uint32_t * graph, size_t rows, size_t dimensions, size_t graph_degree);

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
