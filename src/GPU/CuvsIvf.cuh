#pragma once

#include <cstddef>
#include <cstdint>

/// Only plain types: this is the interface of the libstdc++ island of cuVS.
namespace DB::GPU
{

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
