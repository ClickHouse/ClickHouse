#pragma once

#include <cstddef>
#include <cstdint>

/// Only plain types: this is the interface of the libstdc++ island of cuVS.
namespace DB::GPU
{

/// The tree of the lists of an `ivf` index on the device (see `IvfTree`), which assigns rows to their lists.
class IvfTreeRouter
{
public:
    /// The levels top first: level `l` has `nodes[l]` centroids at `centroids[l]`, and, but for the leaves, `nodes[l] + 1`
    /// offsets of their children in the next level at `child_offsets[l]`. `rotation` is the row-major R of the index, and
    /// `rotated_leaves` the R c of each leaf.
    IvfTreeRouter(size_t dimensions, size_t num_levels, const size_t * nodes, const float * const * centroids,
        const uint32_t * const * child_offsets, const float * rotation, const float * rotated_leaves);
    ~IvfTreeRouter();

    IvfTreeRouter(const IvfTreeRouter &) = delete;
    IvfTreeRouter & operator=(const IvfTreeRouter &) = delete;

    static size_t maxBeam();

    /// Of each of the `rows` vectors, the rows `first_row`, `first_row + 1`, ...: the record of its entry in its list, that is the
    /// nearest leaf by a beam of `beam` nodes on each level above the leaves, the row, and the `IvfQuantization` of
    /// R x - R c, `recordBytes(dimensions)` bytes each.
    void assignAndEncode(const float * vectors, size_t rows, size_t beam, uint32_t first_row, char * records) const;

    static size_t recordBytes(size_t dimensions);

private:
    struct State;
    State * state = nullptr;
};

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
