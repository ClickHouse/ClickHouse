#pragma once
#include <Common/PODArray_fwd.h>
#include <vector>

namespace MemoryAllocationTracker
{

struct Trace
{
    using Frames = std::vector<uintptr_t>;

    Frames frames;
    size_t allocated = 0;
};

using Traces = std::vector<Trace>;

Traces dump_allocations(size_t max_depth, size_t max_bytes, bool only_leafs);

struct DumpTree
{
    struct Node
    {
        uintptr_t id{};
        const void * ptr{};
        size_t allocated{};
    };

    struct Edge
    {
        uintptr_t from{};
        uintptr_t to{};
    };

    using Nodes = std::vector<Node>;
    using Edges = std::vector<Edge>;

    Nodes nodes;
    Edges edges;
};

DumpTree dump_allocations_tree(size_t max_depth, size_t max_bytes);

void dump_allocations_tree(DB::PaddedPODArray<UInt8> & chars, DB::PaddedPODArray<UInt64> & offsets, size_t max_depth, size_t max_bytes);
void dump_allocations_flamegraph(DB::PaddedPODArray<UInt8> & chars, DB::PaddedPODArray<UInt64> & offsets, size_t max_depth, size_t max_bytes);

}
