#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators_pcg_random.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Common/CurrentThread.h>
#include <Common/FunctionDocumentation.h>
#include <Common/PODArray.h>
#include <Common/TargetSpecific.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/threadPoolCallbackRunner.h>

#include <pcg_random.hpp>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <numeric>

/// hierarchicalKMeans(k [, branching] [, max_iter] [, sample_cap] [, seed] [, cosine_distance])(vec)
///
/// Trains k centroids from the aggregated vectors and returns them as Array(Array(Float32)) - the coarse
/// quantizer for a SQL-side IVF index. Keeps a bounded reservoir of sample_cap vectors, so memory is
/// O(sample_cap * dim) whatever the input size; centroids follow the distribution, not the row count.
///
/// Hierarchical k-means avoids running a very large k-means directly.
///
/// For example, to find 32K centroids, instead of comparing every vector
/// against all 32K centroids, we first split the data into a small number
/// of groups (e.g. 16), then split each group again, and continue until
/// we have the requested number of final clusters:
///
///                 1M vectors
///                     │
///                 k-means(16)
///                     │
///          ┌──────────┼──────────┐
///          │          │          │
///        group 0    group 1    ...
///          │          │
///       k-means(16) k-means(16)
///          │          │
///         ...        ...
///          │
///       32K final centroids
///
/// At every level, ordinary k-means is used to split a group into a small
/// number of children. The number of final centroids assigned to each child
/// is proportional to the number of vectors in that child, so large groups
/// receive more centroids than small groups.
///
/// The implementation processes the tree level by level. Near the root there
/// are only a few large groups, so vectors are split across threads. At deeper
/// levels there are many smaller groups, so entire groups can be processed
/// concurrently.
///
/// If a split produces fewer than two non-empty groups (for example, all
/// vectors are identical), we stop splitting and run flat k-means at that
/// node. This also guarantees that the recursion makes progress.
///
/// The result is the requested number of centroids without ever performing
/// a single k-means assignment against the full set of 32K centroids.

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_build_vector_similarity_index_thread_pool_size;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int INCORRECT_DATA;
}

/// Named (not anonymous) so the `TargetSpecific::*` namespaces the macro generates cannot collide with
/// identically named kernels from another translation unit.
namespace HierarchicalKMeansImpl
{
namespace
{

using Float = Float32;

/// The largest coordinate the Float32 scoring math can still handle. Squares are summed in Float32, so a
/// finite but huge coordinate overflows to infinity, every comparison against the running best goes false,
/// and the point keeps whatever centroid the loop started with. `sqrt(FLT_MAX / (4 * dim))` keeps the sum of
/// squares and the dot product finite. At dim = 768 that is ~3.3e17, far above any real embedding.
Float coordinateLimit(size_t dim)
{
    return static_cast<Float>(
        std::sqrt(static_cast<double>(std::numeric_limits<Float>::max()) / (4.0 * static_cast<double>(dim))));
}

DECLARE_MULTITARGET_CODE(

/// Assign every point to its nearest centroid.
///
/// Minimizing `||point - centroid||^2` is the same as minimizing `||centroid||^2 - 2 * dot(point, centroid)`,
/// because `||point||^2` is the same for every candidate. That is why only the centroid norms are needed.
///
/// `centroids_transposed` is column-major, so the inner loop reads `num_centroids` contiguous floats and
/// multiplies them by one broadcast coordinate of the point.
void assignRows(
    const Float * __restrict points, size_t num_points, size_t dim,
    const Float * __restrict centroids_transposed, const Float * __restrict centroid_sq_norms, size_t num_centroids,
    UInt32 * __restrict assignment, Float * __restrict best_score_out)
{
    /// Fixed size so `dots` is a stack array the compiler can keep in registers.
    static constexpr size_t TILE = 32;
    Float dots[TILE];

    for (size_t row = 0; row < num_points; ++row)
    {
        const Float * __restrict point = points + row * dim;
        Float best = std::numeric_limits<Float>::max();
        UInt32 best_centroid = 0;

        for (size_t tile_start = 0; tile_start < num_centroids; tile_start += TILE)
        {
            const size_t width = std::min(TILE, num_centroids - tile_start);

            for (size_t col = 0; col < width; ++col)
                dots[col] = 0.0f;

            for (size_t coord = 0; coord < dim; ++coord)
            {
                const Float coord_value = point[coord];
                const Float * __restrict column = centroids_transposed + coord * num_centroids + tile_start;
                for (size_t col = 0; col < width; ++col)
                    dots[col] += coord_value * column[col];
            }

            for (size_t col = 0; col < width; ++col)
            {
                const Float score = centroid_sq_norms[tile_start + col] - 2.0f * dots[col];
                if (score < best)
                {
                    best = score;
                    best_centroid = static_cast<UInt32>(tile_start + col);
                }
            }
        }

        assignment[row] = best_centroid;
        best_score_out[row] = best;
    }
}

/// Lower `best_sq_dist[row]` to the squared distance from that point to `centroid`, for the k-means++
/// seeding pass. Returns the sum over the range, in double because it feeds the sampling threshold.
double updateMinSqDist(
    const Float * __restrict points, size_t num_points, size_t dim, const Float * __restrict centroid, Float * __restrict best_sq_dist)
{
    double total = 0;
    for (size_t row = 0; row < num_points; ++row)
    {
        const Float * __restrict point = points + row * dim;
        Float sq_dist = 0.0f;
        for (size_t coord = 0; coord < dim; ++coord)
        {
            const Float diff = point[coord] - centroid[coord];
            sq_dist += diff * diff;
        }
        if (sq_dist < best_sq_dist[row])
            best_sq_dist[row] = sq_dist;
        total += static_cast<double>(best_sq_dist[row]);
    }
    return total;
}

/// Add every point into the running sum of the cluster it was assigned to, and count it.
/// The sums are double, not float: they accumulate one term per point, and float would drift.
void accumulateSums(
    const Float * __restrict points, size_t num_points, size_t dim,
    const UInt32 * __restrict assignment, double * __restrict sums, UInt64 * __restrict counts)
{
    for (size_t row = 0; row < num_points; ++row)
    {
        const size_t cluster = assignment[row];
        ++counts[cluster];
        const Float * __restrict point = points + row * dim;
        double * __restrict cluster_sums = sums + cluster * dim;
        for (size_t coord = 0; coord < dim; ++coord)
            cluster_sums[coord] += static_cast<double>(point[coord]);
    }
}

) // DECLARE_MULTITARGET_CODE

/// Runtime dispatch to the widest ISA the CPU supports. Where multitarget code is disabled (ARM, and any
/// build with `ENABLE_MULTITARGET_CODE=OFF`) only `Default` exists, which is why the kernels above are
/// written as plain contiguous loops the compiler can auto-vectorize on its own.
void assignRows(
    const Float * points, size_t num_points, size_t dim, const Float * centroids_transposed, const Float * centroid_sq_norms, size_t num_centroids,
    UInt32 * assignment, Float * best_score_out)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::assignRows(points, num_points, dim, centroids_transposed, centroid_sq_norms, num_centroids, assignment, best_score_out);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::assignRows(points, num_points, dim, centroids_transposed, centroid_sq_norms, num_centroids, assignment, best_score_out);
        return;
    }
#endif
    TargetSpecific::Default::assignRows(points, num_points, dim, centroids_transposed, centroid_sq_norms, num_centroids, assignment, best_score_out);
}

double updateMinSqDist(const Float * points, size_t num_points, size_t dim, const Float * centroid, Float * best_sq_dist)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
        return TargetSpecific::x86_64_v4::updateMinSqDist(points, num_points, dim, centroid, best_sq_dist);
    if (isArchSupported(TargetArch::x86_64_v3))
        return TargetSpecific::x86_64_v3::updateMinSqDist(points, num_points, dim, centroid, best_sq_dist);
#endif
    return TargetSpecific::Default::updateMinSqDist(points, num_points, dim, centroid, best_sq_dist);
}

void accumulateSums(
    const Float * points, size_t num_points, size_t dim, const UInt32 * assignment, double * sums, UInt64 * counts)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::accumulateSums(points, num_points, dim, assignment, sums, counts);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::accumulateSums(points, num_points, dim, assignment, sums, counts);
        return;
    }
#endif
    TargetSpecific::Default::accumulateSums(points, num_points, dim, assignment, sums, counts);
}

/// --- threading helpers ---

/// Training a coarse quantizer is the same kind of work as building a vector similarity index, so it reuses
/// that setting and that global pool. Sharing one pool is what keeps concurrent trainings, or a training
/// running alongside an index build, from oversubscribing the machine.
size_t getMaxTrainingThreads()
{
    size_t threads = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::max_build_vector_similarity_index_thread_pool_size];
    if (threads == 0)
        threads = getNumberOfCPUCoresToUse();
    return std::max<size_t>(threads, 1);
}

ThreadPool & getTrainingThreadPool()
{
    return Context::getGlobalContextInstance()->getBuildVectorSimilarityIndexThreadPool();
}

/// Training a large `k` runs for minutes; without this a `KILL QUERY` could not stop it.
void throwIfKilled()
{
    if (auto query_context = CurrentThread::tryGetQueryContext())
        if (auto query_status = query_context->getProcessListElementSafe())
            query_status->throwIfKilled();
}

/// Split `[0, num_items)` into `num_threads` contiguous ranges and run `body(begin, end, thread_index)` on
/// each. The ranges are fixed up front and every row writes to a slot of its own, so the result does not
/// depend on the order threads finish in. That is what makes training reproducible for a given seed.
template <typename Body>
void parallelRanges(size_t num_items, size_t num_threads, Body && body)
{
    if (num_threads <= 1 || num_items == 0)
    {
        body(0, num_items, 0);
        return;
    }

    const size_t per_thread = (num_items + num_threads - 1) / num_threads;
    ThreadPoolCallbackRunnerLocal<void> runner(getTrainingThreadPool(), ThreadName::MERGETREE_VECTOR_SIM_INDEX);
    for (size_t thread = 0; thread < num_threads; ++thread)
    {
        const size_t begin = thread * per_thread;
        if (begin >= num_items)
            break;
        const size_t end = std::min(num_items, begin + per_thread);
        runner.enqueueAndKeepTrack([&body, begin, end, thread] { body(begin, end, thread); });
    }
    runner.waitForAllToFinishAndRethrowFirstError();
}

struct KMeansParams
{
    size_t iters = 20;
    bool spherical = false;
    /// Threads for the row-parallel loops inside one node. 1 when the caller is already running many nodes
    /// concurrently (see `trainHierarchical`) - the pool is never used re-entrantly.
    size_t num_threads = 1;
};

/// Renormalize centroids to unit length, which turns the L2 argmin into an exact cosine argmin. The
/// guarantee must be absolute or `assignCentroid` ends up ranking against a direction-less centroid.
/// Zero-norm inputs are rejected at `add`, so a zero here is an exactly cancelling mean - substitute e0.
void normalizeCentroids(Float * centroids, size_t num_centroids, size_t dim)
{
    for (size_t cluster = 0; cluster < num_centroids; ++cluster)
    {
        Float * centroid = centroids + cluster * dim;
        double sq_norm = 0;
        for (size_t coord = 0; coord < dim; ++coord)
            sq_norm += static_cast<double>(centroid[coord]) * static_cast<double>(centroid[coord]);

        if (sq_norm > 0)
        {
            const Float inv_norm = static_cast<Float>(1.0 / std::sqrt(sq_norm));
            for (size_t coord = 0; coord < dim; ++coord)
                centroid[coord] *= inv_norm;
        }
        else
        {
            std::fill(centroid, centroid + dim, 0.0f);
            centroid[0] = 1.0f;
        }
    }
}

/// Transpose row-major centroids into the column-major layout `assignRows` wants, and their squared norms.
void transposeCentroids(const Float * row_major, size_t num_centroids, size_t dim, Float * centroids_transposed, Float * centroid_sq_norms)
{
    for (size_t cluster = 0; cluster < num_centroids; ++cluster)
    {
        const Float * centroid = row_major + cluster * dim;
        double sq_norm = 0;
        for (size_t coord = 0; coord < dim; ++coord)
        {
            centroids_transposed[coord * num_centroids + cluster] = centroid[coord];
            sq_norm += static_cast<double>(centroid[coord]) * static_cast<double>(centroid[coord]);
        }
        centroid_sq_norms[cluster] = static_cast<Float>(sq_norm);
    }
}

/// Flat Lloyd k-means: `num_points` points of dimension `dim` in, `num_centroids` row-major centroids out.
/// k-means++ seeding, nearest-centroid assignment via `assignRows`, and reseeding of empty clusters.
VectorWithMemoryTracking<Float> kMeansLloyd(
    const Float * points, size_t num_points, size_t dim, size_t num_centroids, const KMeansParams & params, pcg64 & rng)
{
    num_centroids = std::min(num_centroids, num_points);
    VectorWithMemoryTracking<Float> centroids(num_centroids * dim);
    if (num_centroids == 0)
        return centroids;

    const size_t num_threads = std::max<size_t>(params.num_threads, 1);

    /// --- k-means++ initialization ---
    VectorWithMemoryTracking<Float> best_sq_dist(num_points, std::numeric_limits<Float>::max());
    VectorWithMemoryTracking<double> partial_sums(num_threads, 0.0);
    {
        const size_t first_row = rng() % num_points;
        std::copy(points + first_row * dim, points + (first_row + 1) * dim, centroids.begin());
    }
    for (size_t cluster = 1; cluster < num_centroids; ++cluster)
    {
        const Float * previous_centroid = &centroids[(cluster - 1) * dim];
        std::fill(partial_sums.begin(), partial_sums.end(), 0.0);
        parallelRanges(num_points, num_threads, [&](size_t begin, size_t end, size_t thread)
        {
            partial_sums[thread] = updateMinSqDist(points + begin * dim, end - begin, dim, previous_centroid, best_sq_dist.data() + begin);
        });

        /// Reduced in thread order, not completion order, so the sampling threshold is bit-reproducible.
        double total_sq_dist = 0;
        for (size_t thread = 0; thread < num_threads; ++thread)
            total_sq_dist += partial_sums[thread];

        const double target = (static_cast<double>(rng()) / (static_cast<double>(std::numeric_limits<UInt64>::max()) + 1.0)) * total_sq_dist;
        double running_sum = 0;
        size_t picked_row = num_points - 1;
        for (size_t row = 0; row < num_points; ++row)
        {
            running_sum += static_cast<double>(best_sq_dist[row]);
            if (running_sum >= target)
            {
                picked_row = row;
                break;
            }
        }
        std::copy(points + picked_row * dim, points + (picked_row + 1) * dim, &centroids[cluster * dim]);
    }

    if (params.spherical)
        normalizeCentroids(centroids.data(), num_centroids, dim);

    /// --- Lloyd iterations ---
    /// Two assignment buffers: the kernel overwrites as it goes, and the previous assignment is what tells us
    /// whether anything moved (the convergence test).
    VectorWithMemoryTracking<UInt32> assignment(num_points, std::numeric_limits<UInt32>::max());
    VectorWithMemoryTracking<UInt32> next_assignment(num_points, 0);
    VectorWithMemoryTracking<Float> best_score(num_points, 0.0f);
    VectorWithMemoryTracking<Float> centroids_transposed(dim * num_centroids);
    VectorWithMemoryTracking<Float> centroid_sq_norms(num_centroids);
    VectorWithMemoryTracking<double> sums(num_centroids * dim);
    VectorWithMemoryTracking<UInt64> counts(num_centroids);
    VectorWithMemoryTracking<UInt8> changed_flags(num_threads);

    /// Per-thread accumulation buffers so the mean update is parallel too; leaving it serial would cap the
    /// speedup by Amdahl. Each is `num_centroids * dim` doubles, so a memory budget bounds the threads here.
    static constexpr size_t ACCUM_BUDGET_BYTES = 256 * 1024 * 1024;
    const size_t accum_bytes_per_thread = num_centroids * dim * sizeof(double) + num_centroids * sizeof(UInt64);
    const size_t accum_threads
        = std::clamp<size_t>(ACCUM_BUDGET_BYTES / std::max<size_t>(accum_bytes_per_thread, 1), 1, num_threads);
    VectorWithMemoryTracking<double> thread_sums(accum_threads * num_centroids * dim);
    VectorWithMemoryTracking<UInt64> thread_counts(accum_threads * num_centroids);

    for (size_t iteration = 0; iteration < params.iters; ++iteration)
    {
        throwIfKilled();

        transposeCentroids(centroids.data(), num_centroids, dim, centroids_transposed.data(), centroid_sq_norms.data());

        std::fill(changed_flags.begin(), changed_flags.end(), 0);
        parallelRanges(num_points, num_threads, [&](size_t begin, size_t end, size_t thread)
        {
            assignRows(
                points + begin * dim, end - begin, dim, centroids_transposed.data(), centroid_sq_norms.data(), num_centroids,
                next_assignment.data() + begin, best_score.data() + begin);

            for (size_t row = begin; row < end; ++row)
            {
                if (next_assignment[row] != assignment[row])
                {
                    changed_flags[thread] = 1;
                    break;
                }
            }
        });
        assignment.swap(next_assignment);

        bool changed = false;
        for (size_t thread = 0; thread < num_threads; ++thread)
            changed |= changed_flags[thread] != 0;

        std::fill(thread_sums.begin(), thread_sums.end(), 0.0);
        std::fill(thread_counts.begin(), thread_counts.end(), 0);
        parallelRanges(num_points, accum_threads, [&](size_t begin, size_t end, size_t thread)
        {
            accumulateSums(
                points + begin * dim, end - begin, dim, assignment.data() + begin,
                thread_sums.data() + thread * num_centroids * dim, thread_counts.data() + thread * num_centroids);
        });

        /// Reduced in thread order for reproducibility.
        std::fill(sums.begin(), sums.end(), 0.0);
        std::fill(counts.begin(), counts.end(), 0);
        for (size_t thread = 0; thread < accum_threads; ++thread)
        {
            const double * thread_sum = thread_sums.data() + thread * num_centroids * dim;
            const UInt64 * thread_count = thread_counts.data() + thread * num_centroids;
            for (size_t offset = 0; offset < num_centroids * dim; ++offset)
                sums[offset] += thread_sum[offset];
            for (size_t cluster = 0; cluster < num_centroids; ++cluster)
                counts[cluster] += thread_count[cluster];
        }

        for (size_t cluster = 0; cluster < num_centroids; ++cluster)
        {
            if (counts[cluster] == 0)
            {
                const size_t random_row = rng() % num_points; /// reseed an empty cluster with a random point
                std::copy(points + random_row * dim, points + (random_row + 1) * dim, &centroids[cluster * dim]);
                continue;
            }
            for (size_t coord = 0; coord < dim; ++coord)
                centroids[cluster * dim + coord] = static_cast<Float>(sums[cluster * dim + coord] / static_cast<double>(counts[cluster]));
        }

        if (params.spherical)
            normalizeCentroids(centroids.data(), num_centroids, dim);

        if (!changed && iteration > 0)
            break;
    }

    return centroids;
}

/// Split `num_leaves` leaves across the children, proportional to how many points each holds
/// (largest-remainder apportionment). Two rules: an empty child gets no leaves, and no child gets more leaves
/// than it has points. Leaves displaced by those rules go to children with headroom, so the total is exactly
/// `num_leaves`.
VectorWithMemoryTracking<size_t> apportion(const VectorWithMemoryTracking<size_t> & population, size_t num_leaves)
{
    const size_t num_children = population.size();
    VectorWithMemoryTracking<size_t> leaves(num_children, 0);

    const size_t capacity = std::accumulate(population.begin(), population.end(), static_cast<size_t>(0));
    num_leaves = std::min(num_leaves, capacity); /// cannot produce more centroids than there are points
    if (num_leaves == 0)
        return leaves;

    /// Seed one leaf per non-empty child, largest first, so that a `num_leaves` smaller than the number of
    /// non-empty children goes to the biggest ones.
    VectorWithMemoryTracking<size_t> by_population(num_children);
    std::iota(by_population.begin(), by_population.end(), 0);
    std::sort(by_population.begin(), by_population.end(), [&](size_t lhs, size_t rhs) { return population[lhs] > population[rhs]; });

    size_t placed = 0;
    for (size_t rank = 0; rank < num_children && placed < num_leaves; ++rank)
    {
        const size_t child = by_population[rank];
        if (population[child] == 0)
            break; /// sorted by population, so every child after this one is empty too
        leaves[child] = 1;
        ++placed;
    }

    const size_t remaining = num_leaves - placed;
    if (remaining == 0)
        return leaves;

    VectorWithMemoryTracking<double> fraction(num_children, 0.0);
    size_t placed_by_floor = 0;
    for (size_t child = 0; child < num_children; ++child)
    {
        if (leaves[child] == 0)
            continue;
        const double exact = static_cast<double>(remaining) * static_cast<double>(population[child]) / static_cast<double>(capacity);
        size_t extra = std::min(static_cast<size_t>(std::floor(exact)), population[child] - leaves[child]);
        leaves[child] += extra;
        fraction[child] = exact - std::floor(exact);
        placed_by_floor += extra;
    }

    /// Largest remainder first, then keep sweeping for anything the per-child capacity clamp displaced.
    /// Terminates: every sweep either places a leaf or stops, and total placement is bounded by `capacity`.
    VectorWithMemoryTracking<size_t> by_fraction(num_children);
    std::iota(by_fraction.begin(), by_fraction.end(), 0);
    std::sort(by_fraction.begin(), by_fraction.end(), [&](size_t lhs, size_t rhs) { return fraction[lhs] > fraction[rhs]; });

    size_t still_to_place = remaining - placed_by_floor;
    bool progress = true;
    while (still_to_place > 0 && progress)
    {
        progress = false;
        for (size_t rank = 0; rank < num_children && still_to_place > 0; ++rank)
        {
            const size_t child = by_fraction[rank];
            if (leaves[child] > 0 && leaves[child] < population[child])
            {
                ++leaves[child];
                --still_to_place;
                progress = true;
            }
        }
    }
    return leaves;
}

/// One node of the training tree.
struct TrainTask
{
    /// Indices into the whole sample. Empty together with `all_rows` set means "the entire sample, in order",
    /// which lets the root run straight off the sample instead of copying it (3 GB at 1M x 768).
    VectorWithMemoryTracking<UInt32> rows;
    bool all_rows = false;
    size_t leaves = 0;
    UInt64 seed = 0;
};

using TaskList = VectorWithMemoryTracking<TrainTask>;

/// splitmix64. Each node derives its seed from its parent's seed and its child index, so a node's RNG stream
/// depends only on its position in the tree - never on the order nodes happen to be visited. That is what
/// makes the parallel tree walk produce the same centroids as a serial one.
UInt64 mixSeed(UInt64 seed, size_t child)
{
    UInt64 z = seed + 0x9E3779B97F4A7C15ULL * (static_cast<UInt64>(child) + 1);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/// Run one node: emit whatever leaf centroids it resolves directly, and hand back the children it spawns.
void processTask(
    const TrainTask & task, const Float * sample, size_t sample_rows, size_t dim, size_t branching,
    const KMeansParams & params, VectorWithMemoryTracking<Float> & out_centroids, TaskList & out_children)
{
    const size_t num_points = task.all_rows ? sample_rows : task.rows.size();
    const size_t num_leaves = task.leaves;
    if (num_leaves == 0 || num_points == 0)
        return;

    /// The kernels take a contiguous row-major buffer, so gather this node's rows into one. The root covers
    /// the whole sample and can read it in place.
    VectorWithMemoryTracking<Float> gathered;
    const Float * points = sample;
    if (!task.all_rows)
    {
        gathered.resize(num_points * dim);
        for (size_t row = 0; row < num_points; ++row)
            memcpy(&gathered[row * dim], sample + static_cast<size_t>(task.rows[row]) * dim, dim * sizeof(Float));
        points = gathered.data();
    }

    pcg64 rng(task.seed);

    if (num_leaves >= num_points) /// fewer points than requested leaves: every point becomes a centroid
    {
        out_centroids.insert(out_centroids.end(), points, points + num_points * dim);
        return;
    }

    if (num_leaves <= branching) /// base case: a single flat_centroids num_leaves-means with `num_leaves` clusters
    {
        auto centroids = kMeansLloyd(points, num_points, dim, num_leaves, params, rng);
        out_centroids.insert(out_centroids.end(), centroids.begin(), centroids.end());
        return;
    }

    auto node_centroids = kMeansLloyd(points, num_points, dim, branching, params, rng);

    VectorWithMemoryTracking<Float> centroids_transposed(dim * branching);
    VectorWithMemoryTracking<Float> centroid_sq_norms(branching);
    transposeCentroids(node_centroids.data(), branching, dim, centroids_transposed.data(), centroid_sq_norms.data());

    VectorWithMemoryTracking<UInt32> assignment(num_points);
    VectorWithMemoryTracking<Float> best_score(num_points);
    parallelRanges(num_points, std::max<size_t>(params.num_threads, 1), [&](size_t begin, size_t end, size_t)
    {
        assignRows(
            points + begin * dim, end - begin, dim, centroids_transposed.data(), centroid_sq_norms.data(), branching,
            assignment.data() + begin, best_score.data() + begin);
    });

    VectorWithMemoryTracking<size_t> population(branching, 0);
    for (size_t row = 0; row < num_points; ++row)
        ++population[assignment[row]];

    /// If one child captured every point, the split made no progress and the walk would reproduce this node
    /// forever (all-identical points, say). Emit a flat k-means instead. Past this check every child is
    /// strictly smaller than its parent, which is what bounds the walk.
    size_t non_empty = 0;
    for (size_t cluster = 0; cluster < branching; ++cluster)
        non_empty += (population[cluster] > 0);
    if (non_empty <= 1)
    {
        auto flat_centroids = kMeansLloyd(points, num_points, dim, num_leaves, params, rng);
        out_centroids.insert(out_centroids.end(), flat_centroids.begin(), flat_centroids.end());
        return;
    }

    auto leaves = apportion(population, num_leaves);

    for (size_t cluster = 0; cluster < branching; ++cluster)
    {
        if (leaves[cluster] == 0 || population[cluster] == 0)
            continue;
        if (leaves[cluster] == 1) /// keep the node_centroids centroid itself as the single leaf
        {
            out_centroids.insert(out_centroids.end(), node_centroids.data() + cluster * dim, node_centroids.data() + (cluster + 1) * dim);
            continue;
        }

        TrainTask child;
        child.leaves = leaves[cluster];
        child.seed = mixSeed(task.seed, cluster);
        child.rows.reserve(population[cluster]);
        /// Children carry indices into the WHOLE sample, so a gather is always one hop from the original
        /// buffer rather than a copy of a copy at every level.
        for (size_t row = 0; row < num_points; ++row)
            if (assignment[row] == cluster)
                child.rows.push_back(task.all_rows ? static_cast<UInt32>(row) : task.rows[row]);
        out_children.push_back(std::move(child));
    }
}

/// Breadth-first walk of the training tree, appending the `num_centroids` leaf centroids to `out`. Not recursive: near
/// the root a few nodes hold most points so ROWS split across threads, deep down thousands of tiny NODES run
/// concurrently, and nesting the two would deadlock the pool - so each level picks exactly one regime.
void trainHierarchical(
    const Float * sample, size_t sample_rows, size_t dim, size_t num_centroids, size_t branching,
    size_t iters, bool spherical, UInt64 seed, PaddedPODArray<Float> & out)
{
    if (num_centroids == 0 || sample_rows == 0)
        return;

    const size_t max_threads = getMaxTrainingThreads();

    TaskList level;
    {
        TrainTask root;
        root.all_rows = true;
        root.leaves = num_centroids;
        root.seed = mixSeed(seed, 0); /// same de-correlation as the reservoir RNG
        level.push_back(std::move(root));
    }

    while (!level.empty())
    {
        const size_t num_tasks = level.size();
        VectorWithMemoryTracking<VectorWithMemoryTracking<Float>> level_centroids(num_tasks);
        VectorWithMemoryTracking<TaskList> children(num_tasks);

        if (max_threads > 1 && num_tasks >= max_threads)
        {
            /// Enough independent nodes to fill the pool: one pooled task per node, each node serial inside.
            const KMeansParams params{iters, spherical, 1};
            ThreadPoolCallbackRunnerLocal<void> runner(getTrainingThreadPool(), ThreadName::MERGETREE_VECTOR_SIM_INDEX);
            for (size_t index = 0; index < num_tasks; ++index)
            {
                runner.enqueueAndKeepTrack([&, index]
                {
                    throwIfKilled();
                    processTask(level[index], sample, sample_rows, dim, branching, params, level_centroids[index], children[index]);
                });
            }
            runner.waitForAllToFinishAndRethrowFirstError();
        }
        else
        {
            /// Too few nodes to fill the pool: walk them one at a time, parallelizing over rows instead.
            const KMeansParams params{iters, spherical, max_threads};
            for (size_t index = 0; index < num_tasks; ++index)
            {
                throwIfKilled();
                processTask(level[index], sample, sample_rows, dim, branching, params, level_centroids[index], children[index]);
            }
        }

        /// Concatenated in task order, so the output never depends on completion order.
        for (size_t index = 0; index < num_tasks; ++index)
            out.insert(level_centroids[index].data(), level_centroids[index].data() + level_centroids[index].size());

        TaskList next;
        for (size_t index = 0; index < num_tasks; ++index)
            for (auto & child : children[index])
                next.push_back(std::move(child));
        level = std::move(next);
    }

    /// A node with fewer points than leaves emits raw sample points, which are not unit length. Normalize the
    /// final set so every centroid is a unit direction whichever path produced it.
    if (spherical && !out.empty())
        normalizeCentroids(out.data(), out.size() / dim, dim);
}

/// Aggregate state: a bounded reservoir of training vectors (uniform sample of the input stream).
///
/// The sampling is Vitter's Algorithm R, the textbook reservoir algorithm: keep the first `cap` vectors,
/// then keep the `i`-th vector with probability `cap / i`, replacing a uniformly chosen slot. Every prefix
/// of the stream therefore leaves a uniform sample of everything seen so far.
struct HierarchicalKMeansData
{
    PaddedPODArray<Float> samples; /// flat, (samples.size() / dim) vectors
    UInt64 seen = 0;
    UInt32 dim = 0;

    /// `pcg32_fast`, not `pcg64`, because this generator is serialized and `IO/Operators_pcg_random.h`
    /// only has operators for `pcg32_fast`. The training RNG, which is not serialized, is `pcg64`.
    pcg32_fast rng; /// seeded in create()

    /// Uniform in `[0, limit)`. A `limit` past 2^32 needs a second draw to cover, the way `ReservoirSampler`
    /// does it, because `seen` grows past that on a long stream.
    UInt64 genRandom(UInt64 limit)
    {
        if (limit == 0)
            return 0;
        if (limit <= static_cast<UInt64>(pcg32_fast::max()))
            return rng() % limit;
        return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(pcg32_fast::max()) + 1ULL) + static_cast<UInt64>(rng())) % limit;
    }

    static constexpr size_t no_slot = std::numeric_limits<size_t>::max();

    /// Pick the slot one incoming vector should occupy, or `no_slot` if the reservoir keeps what it has.
    /// The caller writes the vector straight into that slot, so a discarded vector is never copied.
    ///
    /// An empty vector is rejected: `dim == 0` already means "no rows yet", and `samples.size() / dim` would
    /// divide by zero.
    size_t reserveSlot(UInt32 incoming_dim, UInt64 cap)
    {
        if (incoming_dim == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: input vector must not be empty");

        if (dim == 0)
            dim = incoming_dim;
        if (incoming_dim != dim)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "hierarchicalKMeans: got a vector of size {} but expected {}", incoming_dim, dim);

        ++seen;
        const UInt64 have = samples.size() / dim;
        if (have < cap)
        {
            samples.resize(samples.size() + incoming_dim);
            return static_cast<size_t>(have);
        }

        const UInt64 candidate_slot = genRandom(seen); /// Algorithm R reservoir sampling
        return candidate_slot < cap ? static_cast<size_t>(candidate_slot) : no_slot;
    }

    void addVector(const Float * vector, UInt32 incoming_dim, UInt64 cap)
    {
        const size_t slot = reserveSlot(incoming_dim, cap);
        if (slot != no_slot)
            memcpy(&samples[slot * dim], vector, incoming_dim * sizeof(Float));
    }

    /// Same, but pulling coordinates through `read`, so a Float64 or BFloat16 column converts directly into
    /// the reservoir with no intermediate buffer.
    template <typename Reader>
    void addVectorFrom(Reader && read, UInt32 incoming_dim, UInt64 cap)
    {
        const size_t slot = reserveSlot(incoming_dim, cap);
        if (slot == no_slot)
            return;
        Float * destination = &samples[slot * dim];
        for (UInt32 coord = 0; coord < incoming_dim; ++coord)
            destination[coord] = read(coord);
    }

    /// Merge two reservoirs into a uniform sample of their union. How many rows survive from each side is
    /// drawn at random, not fixed to the expected count, which would bias the result and make it depend on
    /// the order the states happen to be merged in.
    void merge(const HierarchicalKMeansData & other, UInt64 cap)
    {
        if (other.dim == 0)
            return;
        if (dim == 0)
        {
            dim = other.dim;
            rng = other.rng;
        }
        if (other.dim != dim)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "hierarchicalKMeans: dim mismatch on merge");

        const UInt64 have_ours = samples.size() / dim;
        const UInt64 have_theirs = other.samples.size() / dim;

        /// Neither side ever dropped a row, so keeping everything is already the uniform sample.
        if (have_ours + have_theirs <= cap)
        {
            samples.insert(other.samples.begin(), other.samples.end());
            seen += other.seen;
            return;
        }

        /// `other` never dropped a row, so replaying its rows one by one gives the same distribution as if
        /// they had arrived on this stream to begin with.
        if (other.seen <= cap)
        {
            for (UInt64 slot = 0; slot < have_theirs; ++slot)
                addVector(&other.samples[slot * dim], dim, cap);
            return;
        }

        /// Symmetric case: we are the side that kept everything, so adopt `other`'s reservoir and replay
        /// our own rows into it.
        if (seen <= cap)
        {
            PaddedPODArray<Float> ours;
            ours.swap(samples);
            samples.insert(other.samples.begin(), other.samples.end());
            seen = other.seen;
            rng = other.rng;
            for (UInt64 slot = 0; slot < have_ours; ++slot)
                addVector(&ours[slot * dim], dim, cap);
            return;
        }

        /// Both sides overflowed, so each holds exactly `cap` rows and only `cap` of the `2 * cap` survive.
        /// How many come from `other` follows the hypergeometric distribution - drawing `cap` times from an
        /// urn without replacement. The loop below is that draw, done directly and in O(cap):
        /// https://en.wikipedia.org/wiki/Hypergeometric_distribution
        UInt64 take_theirs = 0;
        {
            UInt64 remaining_total = seen + other.seen;
            UInt64 remaining_theirs = other.seen;
            for (UInt64 slot = 0; slot < cap; ++slot)
            {
                if (genRandom(remaining_total) < remaining_theirs)
                {
                    ++take_theirs;
                    --remaining_theirs;
                }
                --remaining_total;
            }
        }
        const UInt64 take_ours = cap - take_theirs;

        /// Each side now contributes that many rows, chosen without replacement. A uniform subsample of a
        /// uniform sample is again uniform, so the two halves compose into a uniform sample of the union.

        /// Partial Fisher-Yates over our own rows, moving the survivors to the front.
        for (UInt64 slot = 0; slot < take_ours; ++slot)
        {
            const UInt64 pick = slot + genRandom(have_ours - slot);
            if (pick != slot)
                for (UInt64 coord = 0; coord < dim; ++coord)
                    std::swap(samples[slot * dim + coord], samples[pick * dim + coord]);
        }
        samples.resize(take_ours * dim);

        /// Same for `other`, but it is const, so permute an index array instead of the rows.
        VectorWithMemoryTracking<UInt64> order(have_theirs);
        std::iota(order.begin(), order.end(), 0);
        for (UInt64 slot = 0; slot < take_theirs; ++slot)
        {
            const UInt64 pick = slot + genRandom(have_theirs - slot);
            std::swap(order[slot], order[pick]);
            samples.insert(&other.samples[order[slot] * dim], &other.samples[(order[slot] + 1) * dim]);
        }

        seen += other.seen;
    }
};

class AggregateFunctionHierarchicalKMeans final
    : public IAggregateFunctionDataHelper<HierarchicalKMeansData, AggregateFunctionHierarchicalKMeans>
{
    size_t k;
    size_t branching;
    size_t max_iter;
    UInt64 sample_cap;
    UInt64 seed;
    bool spherical;
    TypeIndex nested_type;

public:
    AggregateFunctionHierarchicalKMeans(const DataTypes & args, const Array & params)
        : IAggregateFunctionDataHelper<HierarchicalKMeansData, AggregateFunctionHierarchicalKMeans>(args, params, createResultType())
    {
        if (params.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function hierarchicalKMeans requires at least the parameter k");

        if (params.size() > 6)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function hierarchicalKMeans accepts at most 6 parameters "
                "(k, branching, max_iter, sample_cap, seed, cosine_distance), got {}", params.size());

        /// Read as a non-negative integer or fail with a message naming the parameter. Going through
        /// `safeGet<UInt64>` alone is not enough: a negative literal is an `Int64` field, so it either raises
        /// a bare `BAD_GET` or, where the value is read anyway, wraps to a huge positive `branching`.
        auto param = [&](size_t i, const char * pname, UInt64 def) -> UInt64
        {
            if (i >= params.size())
                return def;
            if (params[i].getType() != Field::Types::UInt64)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "hierarchicalKMeans: parameter {} must be a non-negative integer, got {}",
                    pname, params[i].dump());
            return params[i].safeGet<UInt64>();
        };

        k          = param(0, "k", 0);
        branching  = param(1, "branching", 16);
        max_iter   = param(2, "max_iter", 20);
        sample_cap = param(3, "sample_cap", 1'000'000);
        seed       = param(4, "seed", 0);
        /// Named `cosine_distance` for the user: normalizing the centroids is what makes the L2 argmin
        /// agree with `cosineDistance`. Internally it stays `spherical`, which is the name of the algorithm.
        spherical  = param(5, "cosine_distance", 0) != 0;

        /// Reject rather than clamp. Silently substituting `branching = 2` for a caller who asked for 1 trains
        /// something other than what was requested, which hides typos and makes experiments irreproducible.
        if (k == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: k must be greater than 0");
        if (branching < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: branching must be at least 2, got {}", branching);
        if (max_iter == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: max_iter must be greater than 0");
        if (sample_cap == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: sample_cap must be greater than 0");
        /// The reservoir holds at most `sample_cap` points and a point yields at most one centroid, so a
        /// smaller cap makes the exact-`k` contract unsatisfiable: training would silently return `sample_cap`
        /// centroids instead of `k`.
        /// `TrainTask::rows` indexes the sample with `UInt32`. A reservoir past that is unreachable anyway -
        /// 2^32 vectors is 12 TB at dim 768 - so cap it rather than double the index memory for a case that
        /// cannot occur.
        if (sample_cap > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "hierarchicalKMeans: sample_cap must not exceed {}, got {}",
                std::numeric_limits<UInt32>::max(), sample_cap);
        if (sample_cap < k)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "hierarchicalKMeans: sample_cap ({}) must be at least k ({}), otherwise the reservoir cannot "
                "hold enough points to train k centroids", sample_cap, k);

        /// Any float width is accepted and converted to the Float32 the kernels use, so a plain array
        /// literal - which is Array(Float64) - works without an explicit CAST.
        const auto * array_type = typeid_cast<const DataTypeArray *>(args[0].get());
        if (!array_type || !isFloat(array_type->getNestedType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function hierarchicalKMeans requires an array of floats");
        nested_type = WhichDataType(array_type->getNestedType()).idx;
    }

    String getName() const override { return "hierarchicalKMeans"; }

    bool allocatesMemoryInArena() const override { return false; }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()));
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) HierarchicalKMeansData();
        /// Hash the seed rather than feeding it in raw. pcg's `oneseq` engines set state directly, so nearby
        /// seeds stay correlated for the first few draws - seeds 1..40 all produced an even first output,
        /// which biased Algorithm R's very first keep/replace decision the same way every time.
        data(place).rng.seed(mixSeed(seed, 0));
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & array = assert_cast<const ColumnArray &>(*columns[0]);
        const auto & offsets = array.getOffsets();
        size_t start = row_num ? offsets[row_num - 1] : 0;
        size_t length = offsets[row_num] - start;

        /// Read one coordinate as Float32, whatever width the column actually holds.
        const IColumn & nested_col = array.getData();
        auto read_coordinate = [&](size_t coord) -> Float
        {
            switch (nested_type)
            {
                case TypeIndex::Float32: return assert_cast<const ColumnFloat32 &>(nested_col).getData()[start + coord];
                case TypeIndex::Float64: return static_cast<Float>(assert_cast<const ColumnFloat64 &>(nested_col).getData()[start + coord]);
                default:                 return static_cast<Float>(assert_cast<const ColumnBFloat16 &>(nested_col).getData()[start + coord]);
            }
        };

        /// One pass that checks the coordinates and computes the norm.
        ///
        /// NaN and Inf are rejected: no comparison against NaN is true, so such a row would land in cluster 0
        /// and could make the trained centroids non-finite. `checkVectorIsSane` in
        /// `MergeTreeIndexVectorSimilarity.cpp` rejects them for the same reason.
        ///
        /// Under `cosine_distance = 1` a zero-norm vector is rejected too, because cosine needs a direction.
        const Float limit = coordinateLimit(length);
        double sq_norm = 0;
        for (size_t coord = 0; coord < length; ++coord)
        {
            const Float value = read_coordinate(coord);
            if (!std::isfinite(value))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "hierarchicalKMeans: input vector must not contain non-finite values (NaN or Inf)");
            if (std::abs(value) > limit)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "hierarchicalKMeans: coordinate {} exceeds the largest magnitude the Float32 training "
                    "math can represent for dimension {} ({})", value, length, limit);
            sq_norm += static_cast<double>(value) * static_cast<double>(value);
        }
        if (spherical && sq_norm == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "hierarchicalKMeans: zero-norm vectors are not allowed with cosine_distance = 1 "
                "(cosine is undefined for a vector with no direction)");

        /// Float32 is copied as is; the other widths convert straight into the reservoir slot.
        if (nested_type == TypeIndex::Float32)
            data(place).addVector(
                &assert_cast<const ColumnFloat32 &>(nested_col).getData()[start], static_cast<UInt32>(length), sample_cap);
        else
            data(place).addVectorFrom(read_coordinate, static_cast<UInt32>(length), sample_cap);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs), sample_cap);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        const auto & state = data(place);
        writeVarUInt(state.dim, buf);
        writeVarUInt(state.seen, buf);
        writeVarUInt(state.samples.size(), buf);
        buf.write(reinterpret_cast<const char *>(state.samples.data()), state.samples.size() * sizeof(Float));

        /// The RNG is part of the state. Without it a deserialized state would resume from a default-constructed
        /// generator, so every shard would replay the same draws and the reservoir would stop being uniform.
        WriteBufferFromOwnString rng_buf;
        rng_buf << state.rng;
        writeStringBinary(rng_buf.str(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        auto & state = data(place);
        readVarUInt(state.dim, buf);
        readVarUInt(state.seen, buf);
        size_t num_values = 0;
        readVarUInt(num_values, buf);

        /// Every later division by `dim` assumes it is non-zero whenever there are rows.
        if (num_values > 0 && (state.dim == 0 || num_values % state.dim != 0))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "hierarchicalKMeans: corrupt aggregate state ({} values for dimension {})", num_values, state.dim);

        /// `hierarchicalKMeansState` hands states to the user, so everything read above is untrusted.
        /// `merge` treats `seen > cap` as proof that the side holds exactly `cap` rows and indexes on that
        /// basis, so the row count has to match `seen` exactly, not merely stay under it.
        const UInt64 have = state.dim ? num_values / state.dim : 0;
        const UInt64 expected = std::min<UInt64>(state.seen, sample_cap);
        if (have != expected)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "hierarchicalKMeans: aggregate state holds {} vectors, but seen = {} with sample_cap = {} "
                "requires exactly {}", have, state.seen, sample_cap, expected);

        state.samples.resize(num_values);
        buf.readStrict(reinterpret_cast<char *>(state.samples.data()), num_values * sizeof(Float));

        /// `add` checks these on the way in, and a transported state never went through `add`.
        const Float limit = state.dim ? coordinateLimit(state.dim) : 0;
        for (UInt64 row = 0; row < have; ++row)
        {
            double sq_norm = 0;
            for (UInt64 coord = 0; coord < state.dim; ++coord)
            {
                const Float value = state.samples[row * state.dim + coord];
                if (!std::isfinite(value))
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "hierarchicalKMeans: aggregate state contains non-finite values (NaN or Inf)");
                if (std::abs(value) > limit)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "hierarchicalKMeans: aggregate state contains coordinate {}, above the largest "
                        "magnitude the Float32 training math can represent for dimension {} ({})",
                        value, state.dim, limit);
                sq_norm += static_cast<double>(value) * static_cast<double>(value);
            }
            if (spherical && sq_norm == 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "hierarchicalKMeans: aggregate state contains a zero-norm vector, which cosine_distance = 1 "
                    "does not allow");
        }

        String rng_string;
        readStringBinary(rng_string, buf);
        ReadBufferFromString rng_buf(rng_string);
        rng_buf >> state.rng;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & state = data(place);
        auto & outer_array = assert_cast<ColumnArray &>(to);
        auto & inner_array = assert_cast<ColumnArray &>(outer_array.getData());
        auto & values = assert_cast<ColumnFloat32 &>(inner_array.getData()).getData();

        if (state.dim == 0) /// empty input -> empty array of centroids
        {
            outer_array.getOffsets().push_back(inner_array.getOffsets().size());
            return;
        }

        PaddedPODArray<Float> centroids;
        trainHierarchical(
            state.samples.data(), state.samples.size() / state.dim, state.dim, k, branching, max_iter, spherical, seed, centroids);

        size_t num_produced = centroids.size() / state.dim;
        for (size_t centroid_index = 0; centroid_index < num_produced; ++centroid_index)
        {
            values.insert(centroids.data() + centroid_index * state.dim, centroids.data() + (centroid_index + 1) * state.dim);
            inner_array.getOffsets().push_back(values.size());
        }
        outer_array.getOffsets().push_back(inner_array.getOffsets().size());
    }
};

AggregateFunctionPtr createAggregateFunctionHierarchicalKMeans(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires exactly one argument (the vector column)", name);
    return std::make_shared<AggregateFunctionHierarchicalKMeans>(argument_types, parameters);
}

}
}

void registerAggregateFunctionHierarchicalKMeans(AggregateFunctionFactory & factory);
void registerAggregateFunctionHierarchicalKMeans(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description =
        "Trains up to `k` cluster centroids from the aggregated vectors using hierarchical k-means and returns "
        "them as `Array(Array(Float32))`. Fewer than `k` are returned only when the input has fewer than `k` rows, "
        "since a row can yield at most one centroid; repeated points still yield `k`. Distance is squared L2; "
        "pass `cosine_distance = 1` to "
        "renormalize the centroids to unit length after each iteration, which makes the same centroids an exact "
        "cosine/inner-product quantizer.";
    FunctionDocumentation::Syntax syntax = "hierarchicalKMeans(k[, branching[, max_iter[, sample_cap[, seed[, cosine_distance]]]]])(vec)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "Vectors to cluster. Every row must have the same dimension, and every coordinate must be finite. "
                "Widths other than `Float32` are converted to `Float32`, which is what the training kernels use.",
         {"Array(Float32)", "Array(Float64)", "Array(BFloat16)"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"k", "Number of centroids to train. Must be greater than 0 and must not exceed `sample_cap`.", {"UInt*"}},
        {"branching", "Optional. Fan-out of the hierarchy: a node that must produce more than `branching` centroids is "
                      "split into `branching` children instead of being clustered directly. Must be at least 2. Default value: 16.",
         {"UInt*"}},
        {"max_iter", "Optional. Upper bound on the number of Lloyd iterations per node; a node stops early once no point "
                     "changes cluster. Must be greater than 0. Default value: 20.", {"UInt*"}},
        {"sample_cap", "Optional. Capacity of the reservoir the aggregate samples into, which bounds the state at "
                       "`sample_cap * dim` floats however many rows are aggregated. Must be at least `k` and at most "
                       "4294967295. Default value: 1000000.", {"UInt*"}},
        {"seed", "Optional. Seed of the reservoir sampling and of the k-means++ initialization. A given seed makes "
                 "training reproducible for a given input order. Default value: 0.", {"UInt*"}},
        {"cosine_distance", "Optional. Set to 1 to renormalize the centroids to unit length after every iteration, "
                            "which makes them an exact cosine/inner-product quantizer. A zero-norm input vector then "
                            "throws an exception rather than being clustered, because cosine is undefined for a vector "
                            "with no direction. Default value: 0.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
        {"An array of up to k centroids, capped by the number of input rows.", {"Array(Array(Float32))"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage",
         "SELECT length(hierarchicalKMeans(4)(vec)) FROM (SELECT [toFloat32(number % 4), toFloat32(number % 4)] AS vec FROM numbers(100))",
         "4"},
        {"Well-separated clusters",
         "SELECT arraySort(hierarchicalKMeans(4)(vec)) FROM (SELECT [toFloat32(intDiv(number, 25)) * 100, toFloat32(0)] AS vec FROM numbers(100))",
         "[[0,0],[100,0],[200,0],[300,0]]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    /// Order-dependent: the `k >= n` shortcut emits points in arrival order, and Algorithm R consumes RNG
    /// draws by stream position. Claiming otherwise lets `removeRedundantSorting` drop an upstream `ORDER BY`
    /// and silently change the trained centroids.
    AggregateFunctionProperties properties = { .is_order_dependent = true };
    factory.registerFunction("hierarchicalKMeans",
        {HierarchicalKMeansImpl::createAggregateFunctionHierarchicalKMeans, documentation, properties});
}

}
