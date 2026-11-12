#pragma once

#include <cstddef>
#include <cstdint>

/// IVF centroids on the device for the `ivf` method of the vector similarity index. Lives on the libstdc++ island of cuVS, so
/// only plain pointers and sizes cross this header, which needs no CUDA headers. Device memory comes from the RMM resource
/// of `GPUDevice`, which must exist.
namespace DB::GPU
{

class IvfCentroids
{
public:
    /// `centroids` is row-major, `num_centroids * dimensions` floats.
    IvfCentroids(const float * centroids, size_t num_centroids, size_t dimensions);
    ~IvfCentroids();

    IvfCentroids(const IvfCentroids &) = delete;
    IvfCentroids & operator=(const IvfCentroids &) = delete;

    /// Writes the position of the nearest (L2) centroid of each of the `rows` host vectors to `positions`.
    /// Calls are serialized; the device memory is bounded by a chunk of rows.
    void assign(const float * vectors, size_t rows, uint32_t * positions) const;

    /// Writes the positions of the `probes` nearest centroids of each of the `num_queries` host queries, nearest first.
    void nearestLists(const float * queries, size_t num_queries, size_t probes, uint32_t * lists) const;

    size_t deviceMemoryBytes() const;

private:
    struct State;
    State * state = nullptr;
};

/// Balanced k-means of cuVS over a sample of `rows` host vectors; writes `num_centroids * dimensions` floats.
void trainIvfCentroids(const float * vectors, size_t rows, size_t dimensions, size_t num_centroids, float * centroids);

enum class IvfDistance : int
{
    /// Squared L2.
    L2 = 0,
    /// `1 - <q, x>` of normalized vectors.
    Cosine = 1,
};

/// Exact distances of a batch of queries to the entries of IVF lists that the host streams to the device in chunks, keeping
/// the best `select` entries of each query on the device between chunks. Only the queries and the best entries stay on the
/// device, so its memory does not grow with the index. Not thread-safe.
class IvfListScan
{
public:
    IvfListScan(const float * queries, size_t num_queries, size_t dimensions, size_t select, IvfDistance distance);
    ~IvfListScan();

    IvfListScan(const IvfListScan &) = delete;
    IvfListScan & operator=(const IvfListScan &) = delete;

    /// The largest `select`.
    static size_t maxSelect();

    /// `chunk` holds `num_lists` lists of `chunk_bytes` in total: list `i` starts at byte `list_begins[i]` with `list_sizes[i]`
    /// entries, their rows (`uint32_t`) and then their vectors (`float`). Query `q` scans the lists
    /// `query_lists[query_list_offsets[q] .. query_list_offsets[q + 1])` of the chunk.
    void scan(const char * chunk, size_t chunk_bytes, const uint64_t * list_begins, const uint32_t * list_sizes, size_t num_lists,
        const uint32_t * query_list_offsets, const uint32_t * query_lists);

    /// The best `select` entries of each query, best first: `num_queries * select` rows and distances; a missing entry is
    /// `std::numeric_limits<uint32_t>::max()` with an infinite distance.
    void result(uint32_t * rows, float * distances) const;

private:
    struct State;
    State * state = nullptr;
};

}
