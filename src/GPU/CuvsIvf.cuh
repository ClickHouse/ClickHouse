#pragma once

#include <cstddef>
#include <cstdint>

/// IVF centroids on the device for the `ivf` method of the vector similarity index. Lives on the libstdc++ island of cuVS, so
/// only plain pointers and sizes cross this header, which needs no CUDA headers. Device memory comes from the RMM resource
/// of `GPUDevice`, which must exist.
namespace DB::GPU
{

/// An entry of an IVF list is a row x of the list of the centroid c, as 4-bit codes of its rotated residual r = R (x - c):
/// component i is `lo + code_i * step`. The entries of a list are laid out as the rows (`uint32_t`), then `lo`, `step` and
/// |r_hat|^2 (three `float`), then the codes: component i of a row in nibble `i % 8` of the 32-bit word `(i % 32) / 8` of the
/// 16-byte word `i / 32`, so that a row is read in 16-byte words.
constexpr size_t ivfCodeBytes(size_t dimensions)
{
    return (dimensions + 31) / 32 * 16;
}

constexpr size_t ivfEntryBytes(size_t dimensions)
{
    return sizeof(uint32_t) + 3 * sizeof(float) + ivfCodeBytes(dimensions);
}

class IvfCentroids
{
public:
    /// `centroids` is row-major, `num_centroids * dimensions` floats.
    IvfCentroids(const float * centroids, size_t num_centroids, size_t dimensions);
    ~IvfCentroids();

    IvfCentroids(const IvfCentroids &) = delete;
    IvfCentroids & operator=(const IvfCentroids &) = delete;

    /// Writes the position of the nearest (L2) centroid of each of the `rows` host vectors to `positions`, and, with
    /// `rotation` (`dimensions x dimensions`, row-major) set, the rotated vectors R x to `rotated`.
    /// Calls are serialized; the device memory is bounded by a chunk of rows.
    void assign(const float * vectors, size_t rows, uint32_t * positions, const float * rotation = nullptr, float * rotated = nullptr) const;

    /// Writes the positions of the `probes` nearest centroids of each of the `num_queries` host queries, nearest first.
    void nearestLists(const float * queries, size_t num_queries, size_t probes, uint32_t * lists) const;

    size_t deviceMemoryBytes() const;

private:
    struct State;
    State * state = nullptr;
};

/// Balanced k-means of cuVS over a sample of `rows` host vectors; writes `num_centroids * dimensions` floats.
void trainIvfCentroids(const float * vectors, size_t rows, size_t dimensions, size_t num_centroids, float * centroids);

/// Approximate L2 distances of a batch of queries to the 4-bit codes of IVF lists (see `ivfEntryBytes`), which the host streams
/// to the device in chunks, keeping the best `select` entries of each query on the device between chunks. Only the queries
/// and the best entries stay on the device, so its memory does not grow with the index. Not thread-safe.
class IvfListScan
{
public:
    /// `rotation` is the R of the codes, `dimensions x dimensions`, row-major.
    IvfListScan(const float * queries, size_t num_queries, size_t dimensions, size_t select, const float * rotation);
    ~IvfListScan();

    IvfListScan(const IvfListScan &) = delete;
    IvfListScan & operator=(const IvfListScan &) = delete;

    /// The largest `select`.
    static size_t maxSelect();

    /// `chunk` holds `num_lists` lists of `chunk_bytes` in total: list `i` starts at byte `list_begins[i]`, a multiple of 16,
    /// with `list_sizes[i]` entries, and its rotated centroid R c is `rotated_centroids[i]`. Query `q` scans the lists
    /// `query_lists[query_list_offsets[q] .. query_list_offsets[q + 1])` of the chunk.
    void scan(const char * chunk, size_t chunk_bytes, const uint64_t * list_begins, const uint32_t * list_sizes, size_t num_lists,
        const float * rotated_centroids, const uint32_t * query_list_offsets, const uint32_t * query_lists);

    /// The best `select` entries of each query by the approximate squared L2 distance, best first: `num_queries * select`
    /// rows and distances; a missing entry is `std::numeric_limits<uint32_t>::max()` with an infinite distance.
    void result(uint32_t * rows, float * distances) const;

private:
    struct State;
    State * state = nullptr;
};

}
