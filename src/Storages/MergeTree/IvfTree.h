#pragma once

#include <base/types.h>

#include <functional>
#include <memory>
#include <vector>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Runs `work(task)` for every task in [0, num_tasks) on the CPU cores, and rethrows the first exception.
void runIvfTasks(size_t num_tasks, const std::function<void(size_t)> & work);

/// The routing tree of the lists of an `ivf` index: the lists are its leaves, and each level above them holds the centroids of
/// groups of the nodes of the next one. A vector reaches its lists by a beam search from the top, so that the work to find the
/// nearest lists grows with the logarithm of their number, not with the number itself. Every leaf has the same depth.
///
/// It is built top-down by k-means on a sample: a node with a budget of L leaves and r levels below it is split into about
/// L^(1/r) children, whose budgets are proportional to their parts of the sample (hierarchical k-means, as in SPANN).
class IvfTree
{
public:
    struct Scratch
    {
        std::vector<std::pair<float, UInt32>> scored;
        std::vector<UInt32> frontier;
    };

    IvfTree() = default;
    explicit IvfTree(size_t dimensions_) : dimensions(dimensions_) {}

    /// At most `num_leaves` leaves (fewer where the sample is too small or a cluster is empty), at most about `branching`
    /// children per node.
    static IvfTree build(const float * sample, size_t sample_rows, size_t dimensions, size_t num_leaves, size_t branching, UInt64 seed);

    size_t leaves() const { return levels.empty() ? 0 : levels.back().nodes(dimensions); }
    size_t depth() const { return levels.size(); }
    const std::vector<float> & leafCentroids() const { return levels.back().centroids; }
    /// The centroids of the nodes of a level, the children of the root first, and the offsets of their children in the next
    /// level (empty for the leaves).
    const std::vector<float> & centroidsOf(size_t level) const { return levels[level].centroids; }
    const std::vector<UInt32> & childOffsetsOf(size_t level) const { return levels[level].child_offsets; }

    /// The `keep` leaves nearest to `vector` (as many as there are, if fewer), nearest first, keeping the `beam` nearest nodes
    /// of every level above the leaves.
    void route(const float * vector, size_t beam, size_t keep, std::vector<UInt32> & result, Scratch & scratch) const;

    void serialize(WriteBuffer & out) const;
    void deserialize(ReadBuffer & in);

    size_t memoryUsageBytes() const;

private:
    struct Level
    {
        std::vector<float> centroids;
        std::vector<float> squared_lengths;
        /// The children of node `i` are the nodes [child_offsets[i], child_offsets[i + 1]) of the next level. Empty for the leaves.
        std::vector<UInt32> child_offsets;

        size_t nodes(size_t dimensions_) const { return centroids.size() / dimensions_; }
    };

    void finish();

    size_t dimensions = 0;
    /// The children of the root first, the leaves last.
    std::vector<Level> levels;
};

}
