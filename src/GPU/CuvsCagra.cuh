#pragma once

#include <GPU/GPUTypes.cuh>

#include <cstddef>
#include <cstdint>

/// CAGRA of cuVS for the `cagra` index. cuVS lives on the libstdc++ island, so only plain pointers and sizes cross this
/// header. Device memory comes from the RMM resource of `GPUDevice`.
namespace DB::GPU
{

enum class CagraDistance : int
{
    /// Squared L2.
    L2 = 0,
    /// Larger is nearer.
    InnerProduct = 1,
};

/// CAGRA-Q search supports only 8-bit PQ codes.
constexpr size_t cagra_pq_codebook_rows = 256;

/// Builds the graph of host vectors on the device; writes `rows * graph_degree` neighbours to `graph`.
__host__ void buildCagraGraph(
    const float * dataset, size_t rows, size_t dimensions, CagraDistance distance,
    size_t graph_degree, size_t intermediate_graph_degree, uint32_t * graph);

/// Host vectors compressed by VPQ for CAGRA-Q: a row is its nearest VQ center plus the residual, coded in pieces of
/// `pq_len` components by one shared PQ codebook. A coded row is the `uint32_t` center and `dimensions / pq_len` code
/// bytes, padded to 4 bytes. Training uses a sample and encoding goes in batches: the dataset is never on the device whole.
class CagraCompressedDataset
{
public:
    __host__ CagraCompressedDataset(const float * dataset, size_t rows, size_t dimensions, size_t pq_len);
    __host__ ~CagraCompressedDataset();

    CagraCompressedDataset(const CagraCompressedDataset &) = delete;
    CagraCompressedDataset & operator=(const CagraCompressedDataset &) = delete;

    __host__ size_t vqCenters() const;
    __host__ size_t rowBytes() const;

    /// Sizes: `vqCenters() * dimensions`, `cagra_pq_codebook_rows * pq_len` floats and `rows * rowBytes()` bytes.
    __host__ void copyToHost(float * vq_codebook, float * pq_codebook, uint8_t * codes) const;

private:
    struct State;
    State * state = nullptr;
};

/// A CAGRA index on the device, uploaded from host vectors and graph. Thread-safe to search.
class CagraIndex
{
public:
    __host__ CagraIndex(const float * dataset, const uint32_t * graph, size_t rows, size_t dimensions, size_t graph_degree,
        CagraDistance distance);

    /// CAGRA-Q over what `CagraCompressedDataset::copyToHost` wrote; searched by squared L2.
    __host__ CagraIndex(const float * vq_codebook, size_t vq_centers, const float * pq_codebook, size_t pq_len,
        const uint8_t * codes, size_t row_bytes, const uint32_t * graph, size_t rows, size_t dimensions, size_t graph_degree);

    __host__ ~CagraIndex();

    CagraIndex(const CagraIndex &) = delete;
    CagraIndex & operator=(const CagraIndex &) = delete;

    /// The `limit` nearest rows to each host query; `candidates` is `itopk_size` (at least `limit`). Writes
    /// `num_queries * limit` rows and distances to the host; a missing row is `std::numeric_limits<uint32_t>::max()`.
    __host__ void search(const float * queries, size_t num_queries, size_t limit, size_t candidates,
        uint32_t * neighbours, float * distances) const;

    __host__ size_t deviceMemoryBytes() const;

private:
    struct State;
    State * state = nullptr;
};

}
