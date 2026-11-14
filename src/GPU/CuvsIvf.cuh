#pragma once

#include <cstddef>
#include <cstdint>

/// Only plain types: this is the interface of the libstdc++ island of cuVS.
namespace DB::GPU
{

/// An entry of a list is R (x - c) in 4 bits per component, `lo + code * step`. A list holds the rows (uint32), then lo,
/// step and |r_hat|^2 (float), then the codes: component i in nibble i % 8 of word (i % 32) / 8 of the 16-byte word i / 32.
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
    IvfCentroids(const float * centroids, size_t num_centroids, size_t dimensions);
    ~IvfCentroids();

    IvfCentroids(const IvfCentroids &) = delete;
    IvfCentroids & operator=(const IvfCentroids &) = delete;

    void assign(const float * vectors, size_t rows, uint32_t * positions, const float * rotation = nullptr, float * rotated = nullptr) const;

    void nearestLists(const float * queries, size_t num_queries, size_t probes, uint32_t * lists) const;

    size_t deviceMemoryBytes() const;

private:
    struct State;
    State * state = nullptr;
};

void trainIvfCentroids(const float * vectors, size_t rows, size_t dimensions, size_t num_centroids, float * centroids);

class IvfListScan
{
public:
    IvfListScan(const float * queries, size_t num_queries, size_t dimensions, size_t select, const float * rotation);
    ~IvfListScan();

    IvfListScan(const IvfListScan &) = delete;
    IvfListScan & operator=(const IvfListScan &) = delete;

    static size_t maxSelect();

    void scan(const char * chunk, size_t chunk_bytes, const uint64_t * list_begins, const uint32_t * list_sizes, size_t num_lists,
        const float * rotated_centroids, const uint32_t * query_list_offsets, const uint32_t * query_lists);

    void result(uint32_t * rows, float * distances) const;

private:
    struct State;
    State * state = nullptr;
};

}
