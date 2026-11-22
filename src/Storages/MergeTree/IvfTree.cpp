#include <Storages/MergeTree/IvfTree.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/getNumberOfCPUCoresToUse.h>

#include <pcg_random.hpp>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <limits>
#include <mutex>
#include <numeric>
#include <thread>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

void runIvfTasks(size_t num_tasks, const std::function<void(size_t)> & work)
{
    const size_t num_threads = std::min<size_t>(getNumberOfCPUCoresToUse(), num_tasks);
    if (num_threads <= 1)
    {
        for (size_t task = 0; task < num_tasks; ++task)
            work(task);
        return;
    }
    std::atomic<size_t> next = 0;
    std::exception_ptr exception;
    std::mutex mutex;
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
        threads.emplace_back([&]
        {
            try
            {
                for (size_t task; (task = next.fetch_add(1)) < num_tasks;)
                    work(task);
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                if (!exception)
                    exception = std::current_exception();
            }
        });
    for (auto & thread : threads)
        thread.join();
    if (exception)
        std::rethrow_exception(exception);
}

namespace
{

/// Eight independent sums, so that the compiler vectorizes the loop without reordering a single sum.
float dot(const float * a, const float * b, size_t dimensions)
{
    float sums[8] = {};
    size_t i = 0;
    for (; i + 8 <= dimensions; i += 8)
        for (size_t j = 0; j < 8; ++j)
            sums[j] += a[i + j] * b[i + j];
    for (; i < dimensions; ++i)
        sums[0] += a[i] * b[i];
    return ((sums[0] + sums[1]) + (sums[2] + sums[3])) + ((sums[4] + sums[5]) + (sums[6] + sums[7]));
}

float squaredLength(const float * a, size_t dimensions)
{
    return dot(a, a, dimensions);
}

/// The nearest of `k` centroids by |c|^2 - 2 x c, which orders them as the L2 distance to `x` does.
UInt32 nearest(const float * x, const float * centroids, const float * squared_lengths, size_t k, size_t dimensions)
{
    UInt32 best = 0;
    float best_score = std::numeric_limits<float>::infinity();
    for (size_t c = 0; c < k; ++c)
    {
        const float score = squared_lengths[c] - 2 * dot(x, centroids + c * dimensions, dimensions);
        if (score < best_score)
        {
            best_score = score;
            best = static_cast<UInt32>(c);
        }
    }
    return best;
}

constexpr size_t kmeans_iterations = 10;
constexpr size_t parallel_points_per_task = 8192;

/// Lloyd's k-means of the sample rows `points` into `k` clusters: the centroids, and the cluster of every point.
void kmeans(const float * sample, const std::vector<UInt32> & points, size_t dimensions, size_t k, pcg64 & rng, bool parallel,
    std::vector<float> & centroids, std::vector<UInt32> & assignment)
{
    const size_t n = points.size();
    centroids.assign(k * dimensions, 0);
    assignment.assign(n, 0);
    if (k == 1)
    {
        for (UInt32 point : points)
            for (size_t i = 0; i < dimensions; ++i)
                centroids[i] += sample[size_t(point) * dimensions + i];
        for (size_t i = 0; i < dimensions; ++i)
            centroids[i] /= static_cast<float>(n);
        return;
    }

    /// Distinct points as the initial centroids.
    std::vector<UInt32> order(n);
    std::iota(order.begin(), order.end(), 0);
    for (size_t c = 0; c < k; ++c)
    {
        std::swap(order[c], order[c + rng() % (n - c)]);
        std::copy_n(sample + size_t(points[order[c]]) * dimensions, dimensions, centroids.begin() + c * dimensions);
    }

    std::vector<float> squared_lengths(k);
    std::vector<double> sums(k * dimensions);
    std::vector<size_t> counts(k);
    const size_t num_tasks = parallel ? (n + parallel_points_per_task - 1) / parallel_points_per_task : 1;
    const size_t task_points = (n + num_tasks - 1) / num_tasks;
    for (size_t iteration = 0; iteration <= kmeans_iterations; ++iteration)
    {
        for (size_t c = 0; c < k; ++c)
            squared_lengths[c] = squaredLength(centroids.data() + c * dimensions, dimensions);

        auto assign = [&](size_t task)
        {
            for (size_t i = task * task_points; i < std::min(n, (task + 1) * task_points); ++i)
                assignment[i] = nearest(sample + size_t(points[i]) * dimensions, centroids.data(), squared_lengths.data(), k, dimensions);
        };
        if (num_tasks > 1)
            runIvfTasks(num_tasks, assign);
        else
            assign(0);

        if (iteration == kmeans_iterations)
            break;

        std::fill(sums.begin(), sums.end(), 0.0);
        std::fill(counts.begin(), counts.end(), 0);
        for (size_t i = 0; i < n; ++i)
        {
            const float * x = sample + size_t(points[i]) * dimensions;
            double * sum = sums.data() + size_t(assignment[i]) * dimensions;
            for (size_t j = 0; j < dimensions; ++j)
                sum[j] += static_cast<double>(x[j]);
            ++counts[assignment[i]];
        }
        const size_t largest = std::max_element(counts.begin(), counts.end()) - counts.begin();
        for (size_t c = 0; c < k; ++c)
        {
            float * centroid = centroids.data() + c * dimensions;
            if (counts[c] == 0)
            {
                /// An empty cluster takes a point of the largest one, slightly moved off it.
                size_t taken = rng() % n;
                for (size_t attempt = 0; attempt < 64 && assignment[taken] != largest; ++attempt)
                    taken = rng() % n;
                const float * x = sample + size_t(points[taken]) * dimensions;
                for (size_t j = 0; j < dimensions; ++j)
                    centroid[j] = x[j] * (1.0f + 1e-3f * (static_cast<float>(rng() % 2001) / 1000.0f - 1.0f));
                continue;
            }
            for (size_t j = 0; j < dimensions; ++j)
                centroid[j] = static_cast<float>(sums[c * dimensions + j] / static_cast<double>(counts[c]));
        }
    }
}

struct BuildNode
{
    std::vector<float> centroid;
    std::vector<std::unique_ptr<BuildNode>> children;
};

struct Builder
{
    const float * sample;
    size_t dimensions;
    UInt64 seed;

    /// Splits `node`, whose rows of the sample are `points`, into a subtree of `remaining` levels with at most `budget` leaves.
    void split(BuildNode & node, const std::vector<UInt32> & points, size_t budget, size_t remaining, bool parallel, UInt64 node_seed) const
    {
        if (remaining == 0)
            return;
        budget = std::clamp<size_t>(budget, 1, points.size());
        size_t k = remaining == 1 ? budget
            : std::clamp<size_t>(static_cast<size_t>(std::ceil(std::pow(static_cast<double>(budget), 1.0 / static_cast<double>(remaining)) - 1e-9)), 1, budget);

        pcg64 rng(node_seed);
        std::vector<float> centroids;
        std::vector<UInt32> assignment;
        kmeans(sample, points, dimensions, k, rng, parallel, centroids, assignment);

        std::vector<std::vector<UInt32>> parts(k);
        for (size_t i = 0; i < points.size(); ++i)
            parts[assignment[i]].push_back(points[i]);

        std::vector<std::vector<UInt32>> kept_parts;
        for (size_t c = 0; c < k; ++c)
        {
            if (parts[c].empty())
                continue;
            auto child = std::make_unique<BuildNode>();
            child->centroid.assign(centroids.begin() + c * dimensions, centroids.begin() + (c + 1) * dimensions);
            node.children.push_back(std::move(child));
            kept_parts.push_back(std::move(parts[c]));
        }

        if (remaining == 1)
            return;

        auto split_child = [&](size_t c)
        {
            const size_t child_budget = std::max<size_t>(1, static_cast<size_t>(std::llround(
                static_cast<double>(budget) * static_cast<double>(kept_parts[c].size()) / static_cast<double>(points.size()))));
            split(*node.children[c], kept_parts[c], child_budget, remaining - 1, /*parallel=*/ false, node_seed * 1000003 + c + 1);
        };
        if (parallel)
            runIvfTasks(node.children.size(), split_child);
        else
            for (size_t c = 0; c < node.children.size(); ++c)
                split_child(c);
    }
};

}

IvfTree IvfTree::build(const float * sample, size_t sample_rows, size_t dimensions, size_t num_leaves, size_t branching, UInt64 seed)
{
    if (sample_rows == 0 || num_leaves == 0 || dimensions == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "An IVF tree is built of no rows, no leaves or no dimensions");
    if (sample_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "An IVF tree is built of too many rows of a sample: {}", sample_rows);

    num_leaves = std::min(num_leaves, sample_rows);
    branching = std::max<size_t>(branching, 2);
    size_t depth = 1;
    for (double capacity = static_cast<double>(branching); capacity < static_cast<double>(num_leaves); capacity *= static_cast<double>(branching))
        ++depth;

    BuildNode root;
    std::vector<UInt32> points(sample_rows);
    std::iota(points.begin(), points.end(), 0);
    Builder{sample, dimensions, seed}.split(root, points, num_leaves, depth, /*parallel=*/ true, seed);

    /// The levels, breadth first: as every leaf has the same depth, the leaves are in the order of a depth-first walk, so
    /// that the leaves of a node are adjacent.
    IvfTree tree(dimensions);
    tree.levels.resize(depth);
    std::vector<const BuildNode *> current;
    for (const auto & child : root.children)
        current.push_back(child.get());
    for (size_t level = 0; level < depth; ++level)
    {
        auto & out = tree.levels[level];
        std::vector<const BuildNode *> next;
        out.centroids.reserve(current.size() * dimensions);
        if (level + 1 < depth)
            out.child_offsets.push_back(0);
        for (const auto * node : current)
        {
            out.centroids.insert(out.centroids.end(), node->centroid.begin(), node->centroid.end());
            if (level + 1 < depth)
            {
                if (node->children.empty())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "A node of an IVF tree above the leaves has no children");
                for (const auto & child : node->children)
                    next.push_back(child.get());
                out.child_offsets.push_back(static_cast<UInt32>(next.size()));
            }
        }
        current = std::move(next);
    }
    tree.finish();
    return tree;
}

void IvfTree::finish()
{
    for (auto & level : levels)
    {
        const size_t nodes = level.nodes(dimensions);
        level.squared_lengths.resize(nodes);
        for (size_t i = 0; i < nodes; ++i)
            level.squared_lengths[i] = squaredLength(level.centroids.data() + i * dimensions, dimensions);
    }
}

void IvfTree::route(const float * vector, size_t beam, size_t keep, std::vector<UInt32> & result, Scratch & scratch) const
{
    result.clear();
    if (levels.empty() || keep == 0)
        return;
    beam = std::max<size_t>(beam, 1);

    auto & frontier = scratch.frontier;
    auto & scored = scratch.scored;
    frontier.resize(levels[0].nodes(dimensions));
    std::iota(frontier.begin(), frontier.end(), 0);
    for (size_t l = 0; l < levels.size(); ++l)
    {
        const auto & level = levels[l];
        scored.clear();
        for (UInt32 node : frontier)
            scored.emplace_back(level.squared_lengths[node] - 2 * dot(vector, level.centroids.data() + size_t(node) * dimensions, dimensions), node);

        const bool is_leaves = l + 1 == levels.size();
        const size_t take = std::min(scored.size(), is_leaves ? keep : beam);
        std::partial_sort(scored.begin(), scored.begin() + take, scored.end());
        if (is_leaves)
        {
            for (size_t i = 0; i < take; ++i)
                result.push_back(scored[i].second);
            return;
        }

        frontier.clear();
        for (size_t i = 0; i < take; ++i)
            for (UInt32 child = level.child_offsets[scored[i].second]; child < level.child_offsets[scored[i].second + 1]; ++child)
                frontier.push_back(child);
    }
}

void IvfTree::serialize(WriteBuffer & out) const
{
    writeIntBinary(static_cast<UInt64>(levels.size()), out);
    for (const auto & level : levels)
    {
        writeIntBinary(static_cast<UInt64>(level.nodes(dimensions)), out);
        out.write(reinterpret_cast<const char *>(level.centroids.data()), level.centroids.size() * sizeof(float));
        out.write(reinterpret_cast<const char *>(level.child_offsets.data()), level.child_offsets.size() * sizeof(UInt32));
    }
}

void IvfTree::deserialize(ReadBuffer & in)
{
    UInt64 depth = 0;
    readIntBinary(depth, in);
    if (depth == 0 || depth > 64)
        throw Exception(ErrorCodes::INCORRECT_DATA, "An IVF tree has an invalid depth {}", depth);
    levels.assign(depth, {});
    for (size_t l = 0; l < depth; ++l)
    {
        UInt64 nodes = 0;
        readIntBinary(nodes, in);
        if (nodes == 0 || nodes > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::INCORRECT_DATA, "A level of an IVF tree has an invalid number of nodes {}", nodes);
        auto & level = levels[l];
        level.centroids.resize(nodes * dimensions);
        in.readStrict(reinterpret_cast<char *>(level.centroids.data()), level.centroids.size() * sizeof(float));
        if (l + 1 < depth)
        {
            level.child_offsets.resize(nodes + 1);
            in.readStrict(reinterpret_cast<char *>(level.child_offsets.data()), level.child_offsets.size() * sizeof(UInt32));
        }
    }
    for (size_t l = 0; l + 1 < depth; ++l)
    {
        const auto & offsets = levels[l].child_offsets;
        if (offsets.front() != 0 || offsets.back() != levels[l + 1].nodes(dimensions)
            || std::adjacent_find(offsets.begin(), offsets.end(), std::greater_equal<UInt32>()) != offsets.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "A level of an IVF tree has invalid offsets of the children");
    }
    finish();
}

size_t IvfTree::memoryUsageBytes() const
{
    size_t bytes = 0;
    for (const auto & level : levels)
        bytes += (level.centroids.size() + level.squared_lengths.size()) * sizeof(float) + level.child_offsets.size() * sizeof(UInt32);
    return bytes;
}

}
