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

/// hierarchicalKMeans(k [, branching] [, max_iter] [, sample_cap] [, seed] [, spherical])(vec)
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

/// Coordinates are squared and summed in Float32, so a finite but very large value can still overflow the
/// accumulator to infinity. Every `score < best` comparison then goes false and the point silently takes
/// whatever id the loop started with. Bounding `|x|` by `sqrt(FLT_MAX / (4 * dim))` keeps the sum of squares,
/// the dot product and `cnorm - 2 * dot` all finite. At dim = 768 the bound is ~3.3e17, far above any real
/// embedding, so this rejects only input the kernels could not have scored correctly anyway.
Float coordinateLimit(size_t dim)
{
    return static_cast<Float>(
        std::sqrt(static_cast<double>(std::numeric_limits<Float>::max()) / (4.0 * static_cast<double>(dim))));
}

DECLARE_MULTITARGET_CODE(

/// For every point, `argmin_c (||c||^2 - 2 x.c)` - that is `argmin_c ||x - c||^2` with the constant `||x||^2`
/// dropped. `ct` is column-major (`ct[j * k + c]`), so the inner loop walks `k` contiguous floats against a
/// broadcast `x[j]` and the `k` accumulators stay in registers. In the tree `k == branching`, 16 by default.
void assignRows(
    const Float * __restrict pts, size_t n, size_t d,
    const Float * __restrict ct, const Float * __restrict cnorm, size_t k,
    UInt32 * __restrict assign, Float * __restrict best_out)
{
    /// Fixed so `acc` is a stack array the compiler can keep in registers.
    static constexpr size_t TILE = 32;
    Float acc[TILE];

    for (size_t i = 0; i < n; ++i)
    {
        const Float * __restrict x = pts + i * d;
        Float best = std::numeric_limits<Float>::max();
        UInt32 best_c = 0;

        for (size_t c0 = 0; c0 < k; c0 += TILE)
        {
            const size_t width = std::min(TILE, k - c0);

            for (size_t c = 0; c < width; ++c)
                acc[c] = 0.0f;

            for (size_t j = 0; j < d; ++j)
            {
                const Float xj = x[j];
                const Float * __restrict col = ct + j * k + c0;
                for (size_t c = 0; c < width; ++c)
                    acc[c] += xj * col[c];
            }

            for (size_t c = 0; c < width; ++c)
            {
                const Float score = cnorm[c0 + c] - 2.0f * acc[c];
                if (score < best)
                {
                    best = score;
                    best_c = static_cast<UInt32>(c0 + c);
                }
            }
        }

        assign[i] = best_c;
        best_out[i] = best;
    }
}

/// `best_d2[i] = min(best_d2[i], ||x_i - cen||^2)` for the k-means++ seeding pass. Returns `sum(best_d2)`
/// over the range, accumulated in double because it feeds the sampling threshold.
double updateMinSqDist(
    const Float * __restrict pts, size_t n, size_t d, const Float * __restrict cen, Float * __restrict best_d2)
{
    double total = 0;
    for (size_t i = 0; i < n; ++i)
    {
        const Float * __restrict x = pts + i * d;
        Float dd = 0.0f;
        for (size_t j = 0; j < d; ++j)
        {
            const Float t = x[j] - cen[j];
            dd += t * t;
        }
        if (dd < best_d2[i])
            best_d2[i] = dd;
        total += static_cast<double>(best_d2[i]);
    }
    return total;
}

/// `sums[c * d + j] += x_i[j]` and `++counts[c]` for the cluster each point was assigned to.
/// `sums` stays double: it accumulates up to `n` values per coordinate, where float would drift.
void accumulateSums(
    const Float * __restrict pts, size_t n, size_t d,
    const UInt32 * __restrict assign, double * __restrict sums, UInt64 * __restrict counts)
{
    for (size_t i = 0; i < n; ++i)
    {
        const size_t c = assign[i];
        ++counts[c];
        const Float * __restrict x = pts + i * d;
        double * __restrict s = sums + c * d;
        for (size_t j = 0; j < d; ++j)
            s[j] += static_cast<double>(x[j]);
    }
}

) // DECLARE_MULTITARGET_CODE

/// Runtime dispatch to the widest ISA the CPU supports. Where multitarget code is disabled (ARM, and any
/// build with `ENABLE_MULTITARGET_CODE=OFF`) only `Default` exists, which is why the kernels above are
/// written as plain contiguous loops the compiler can auto-vectorize on its own.
void assignRows(
    const Float * pts, size_t n, size_t d, const Float * ct, const Float * cnorm, size_t k,
    UInt32 * assign, Float * best_out)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::assignRows(pts, n, d, ct, cnorm, k, assign, best_out);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::assignRows(pts, n, d, ct, cnorm, k, assign, best_out);
        return;
    }
#endif
    TargetSpecific::Default::assignRows(pts, n, d, ct, cnorm, k, assign, best_out);
}

double updateMinSqDist(const Float * pts, size_t n, size_t d, const Float * cen, Float * best_d2)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
        return TargetSpecific::x86_64_v4::updateMinSqDist(pts, n, d, cen, best_d2);
    if (isArchSupported(TargetArch::x86_64_v3))
        return TargetSpecific::x86_64_v3::updateMinSqDist(pts, n, d, cen, best_d2);
#endif
    return TargetSpecific::Default::updateMinSqDist(pts, n, d, cen, best_d2);
}

void accumulateSums(
    const Float * pts, size_t n, size_t d, const UInt32 * assign, double * sums, UInt64 * counts)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        TargetSpecific::x86_64_v4::accumulateSums(pts, n, d, assign, sums, counts);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::accumulateSums(pts, n, d, assign, sums, counts);
        return;
    }
#endif
    TargetSpecific::Default::accumulateSums(pts, n, d, assign, sums, counts);
}

/// --- threading helpers ---

/// Training the IVF coarse quantizer is the same kind of work as building a vector similarity index, so it
/// reuses that setting and that global pool - sharing the pool is what stops several concurrent trainings
/// (or a training racing a merge that builds an index) from oversubscribing the box.
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

/// Split `[0, n)` into `num_threads` fixed contiguous ranges and run `body(begin, end, thread_index)` on each.
/// The ranges and the output slot of every row are fixed up front, so results never depend on scheduling -
/// that is what keeps training reproducible for a given seed.
template <typename Body>
void parallelRanges(size_t n, size_t num_threads, Body && body)
{
    if (num_threads <= 1 || n == 0)
    {
        body(0, n, 0);
        return;
    }

    const size_t per_thread = (n + num_threads - 1) / num_threads;
    ThreadPoolCallbackRunnerLocal<void> runner(getTrainingThreadPool(), ThreadName::MERGETREE_VECTOR_SIM_INDEX);
    for (size_t t = 0; t < num_threads; ++t)
    {
        const size_t begin = t * per_thread;
        if (begin >= n)
            break;
        const size_t end = std::min(n, begin + per_thread);
        runner.enqueueAndKeepTrack([&body, begin, end, t] { body(begin, end, t); });
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
void normalizeCentroids(Float * centroids, size_t k, size_t d)
{
    for (size_t c = 0; c < k; ++c)
    {
        Float * cen = centroids + c * d;
        double norm2 = 0;
        for (size_t j = 0; j < d; ++j)
            norm2 += static_cast<double>(cen[j]) * static_cast<double>(cen[j]);

        if (norm2 > 0)
        {
            const Float inv = static_cast<Float>(1.0 / std::sqrt(norm2));
            for (size_t j = 0; j < d; ++j)
                cen[j] *= inv;
        }
        else
        {
            std::fill(cen, cen + d, 0.0f);
            cen[0] = 1.0f;
        }
    }
}

/// Pack row-major centroids into the column-major layout `assignRows` wants, and their squared norms.
void packCentroids(const Float * rows, size_t k, size_t d, Float * ct, Float * cnorm)
{
    for (size_t c = 0; c < k; ++c)
    {
        const Float * cen = rows + c * d;
        double s = 0;
        for (size_t j = 0; j < d; ++j)
        {
            ct[j * k + c] = cen[j];
            s += static_cast<double>(cen[j]) * static_cast<double>(cen[j]);
        }
        cnorm[c] = static_cast<Float>(s);
    }
}

/// Flat Lloyd k-means: `n` points of dimension `d` -> `k` row-major centroids (`k * d` floats).
/// k-means++ seeding, argmin via the reformulation above, and empty-cluster reseeding.
VectorWithMemoryTracking<Float> kMeansLloyd(
    const Float * pts, size_t n, size_t d, size_t k, const KMeansParams & params, pcg64 & rng)
{
    k = std::min(k, n);
    VectorWithMemoryTracking<Float> centroids(k * d);
    if (k == 0)
        return centroids;

    const size_t num_threads = std::max<size_t>(params.num_threads, 1);

    /// --- k-means++ initialization ---
    VectorWithMemoryTracking<Float> best_d2(n, std::numeric_limits<Float>::max());
    VectorWithMemoryTracking<double> partial(num_threads, 0.0);
    {
        const size_t first = rng() % n;
        std::copy(pts + first * d, pts + (first + 1) * d, centroids.begin());
    }
    for (size_t c = 1; c < k; ++c)
    {
        const Float * prev = &centroids[(c - 1) * d];
        std::fill(partial.begin(), partial.end(), 0.0);
        parallelRanges(n, num_threads, [&](size_t begin, size_t end, size_t t)
        {
            partial[t] = updateMinSqDist(pts + begin * d, end - begin, d, prev, best_d2.data() + begin);
        });

        /// Reduced in thread order, not completion order, so the sampling threshold is bit-reproducible.
        double sum = 0;
        for (size_t t = 0; t < num_threads; ++t)
            sum += partial[t];

        const double r = (static_cast<double>(rng()) / (static_cast<double>(std::numeric_limits<UInt64>::max()) + 1.0)) * sum;
        double acc = 0;
        size_t pick = n - 1;
        for (size_t i = 0; i < n; ++i)
        {
            acc += static_cast<double>(best_d2[i]);
            if (acc >= r)
            {
                pick = i;
                break;
            }
        }
        std::copy(pts + pick * d, pts + (pick + 1) * d, &centroids[c * d]);
    }

    if (params.spherical)
        normalizeCentroids(centroids.data(), k, d);

    /// --- Lloyd iterations ---
    /// Two assignment buffers: the kernel overwrites as it goes, and the previous assignment is what tells us
    /// whether anything moved (the convergence test).
    VectorWithMemoryTracking<UInt32> assign(n, std::numeric_limits<UInt32>::max());
    VectorWithMemoryTracking<UInt32> next_assign(n, 0);
    VectorWithMemoryTracking<Float> best_score(n, 0.0f);
    VectorWithMemoryTracking<Float> ct(d * k);
    VectorWithMemoryTracking<Float> cnorm(k);
    VectorWithMemoryTracking<double> sums(k * d);
    VectorWithMemoryTracking<UInt64> counts(k);
    VectorWithMemoryTracking<UInt8> changed_flags(num_threads);

    /// Per-thread accumulation buffers so the mean update is parallel too; leaving it serial would cap the
    /// speedup by Amdahl. Each is `k * d` doubles, so bound the threads for this step by a memory budget.
    static constexpr size_t ACCUM_BUDGET_BYTES = 256 * 1024 * 1024;
    const size_t accum_bytes_per_thread = k * d * sizeof(double) + k * sizeof(UInt64);
    const size_t accum_threads
        = std::clamp<size_t>(ACCUM_BUDGET_BYTES / std::max<size_t>(accum_bytes_per_thread, 1), 1, num_threads);
    VectorWithMemoryTracking<double> tsums(accum_threads * k * d);
    VectorWithMemoryTracking<UInt64> tcounts(accum_threads * k);

    for (size_t iteration = 0; iteration < params.iters; ++iteration)
    {
        throwIfKilled();

        packCentroids(centroids.data(), k, d, ct.data(), cnorm.data());

        std::fill(changed_flags.begin(), changed_flags.end(), 0);
        parallelRanges(n, num_threads, [&](size_t begin, size_t end, size_t t)
        {
            assignRows(
                pts + begin * d, end - begin, d, ct.data(), cnorm.data(), k,
                next_assign.data() + begin, best_score.data() + begin);

            for (size_t i = begin; i < end; ++i)
            {
                if (next_assign[i] != assign[i])
                {
                    changed_flags[t] = 1;
                    break;
                }
            }
        });
        assign.swap(next_assign);

        bool changed = false;
        for (size_t t = 0; t < num_threads; ++t)
            changed |= changed_flags[t] != 0;

        std::fill(tsums.begin(), tsums.end(), 0.0);
        std::fill(tcounts.begin(), tcounts.end(), 0);
        parallelRanges(n, accum_threads, [&](size_t begin, size_t end, size_t t)
        {
            accumulateSums(
                pts + begin * d, end - begin, d, assign.data() + begin,
                tsums.data() + t * k * d, tcounts.data() + t * k);
        });

        /// Reduced in thread order for reproducibility.
        std::fill(sums.begin(), sums.end(), 0.0);
        std::fill(counts.begin(), counts.end(), 0);
        for (size_t t = 0; t < accum_threads; ++t)
        {
            const double * ts = tsums.data() + t * k * d;
            const UInt64 * tc = tcounts.data() + t * k;
            for (size_t idx = 0; idx < k * d; ++idx)
                sums[idx] += ts[idx];
            for (size_t c = 0; c < k; ++c)
                counts[c] += tc[c];
        }

        for (size_t c = 0; c < k; ++c)
        {
            if (counts[c] == 0)
            {
                const size_t r = rng() % n; /// reseed an empty cluster with a random point
                std::copy(pts + r * d, pts + (r + 1) * d, &centroids[c * d]);
                continue;
            }
            for (size_t j = 0; j < d; ++j)
                centroids[c * d + j] = static_cast<Float>(sums[c * d + j] / static_cast<double>(counts[c]));
        }

        if (params.spherical)
            normalizeCentroids(centroids.data(), k, d);

        if (!changed && iteration > 0)
            break;
    }

    return centroids;
}

/// Split `k` leaves across the `B` children, proportional to population (largest-remainder). Two rules, and
/// breaking either silently loses centroids: a child with no points gets no leaves, and no child gets more
/// leaves than points. What that displaces goes to children with headroom, so the total is exactly `k`.
VectorWithMemoryTracking<size_t> apportion(const VectorWithMemoryTracking<size_t> & pop, size_t k)
{
    const size_t B = pop.size();
    VectorWithMemoryTracking<size_t> leaves(B, 0);

    const size_t capacity = std::accumulate(pop.begin(), pop.end(), static_cast<size_t>(0));
    k = std::min(k, capacity); /// cannot produce more centroids than there are points
    if (k == 0)
        return leaves;

    /// Seed one leaf per non-empty child, largest first, so that a `k` smaller than the number of non-empty
    /// children goes to the biggest ones.
    VectorWithMemoryTracking<size_t> by_pop(B);
    std::iota(by_pop.begin(), by_pop.end(), 0);
    std::sort(by_pop.begin(), by_pop.end(), [&](size_t a, size_t b) { return pop[a] > pop[b]; });

    size_t placed = 0;
    for (size_t i = 0; i < B && placed < k; ++i)
    {
        const size_t c = by_pop[i];
        if (pop[c] == 0)
            break; /// sorted by population, so every child after this one is empty too
        leaves[c] = 1;
        ++placed;
    }

    const size_t remaining = k - placed;
    if (remaining == 0)
        return leaves;

    VectorWithMemoryTracking<double> frac(B, 0.0);
    size_t handed = 0;
    for (size_t c = 0; c < B; ++c)
    {
        if (leaves[c] == 0)
            continue;
        const double exact = static_cast<double>(remaining) * static_cast<double>(pop[c]) / static_cast<double>(capacity);
        size_t add = std::min(static_cast<size_t>(std::floor(exact)), pop[c] - leaves[c]);
        leaves[c] += add;
        frac[c] = exact - std::floor(exact);
        handed += add;
    }

    /// Largest remainder first, then keep sweeping for anything the per-child capacity clamp displaced.
    /// Terminates: every sweep either places a leaf or stops, and total placement is bounded by `capacity`.
    VectorWithMemoryTracking<size_t> by_frac(B);
    std::iota(by_frac.begin(), by_frac.end(), 0);
    std::sort(by_frac.begin(), by_frac.end(), [&](size_t a, size_t b) { return frac[a] > frac[b]; });

    size_t left = remaining - handed;
    bool progress = true;
    while (left > 0 && progress)
    {
        progress = false;
        for (size_t i = 0; i < B && left > 0; ++i)
        {
            const size_t c = by_frac[i];
            if (leaves[c] > 0 && leaves[c] < pop[c])
            {
                ++leaves[c];
                --left;
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
    const TrainTask & task, const Float * sample, size_t sample_rows, size_t d, size_t branching,
    const KMeansParams & params, VectorWithMemoryTracking<Float> & out_centroids, TaskList & out_children)
{
    const size_t n = task.all_rows ? sample_rows : task.rows.size();
    const size_t k = task.leaves;
    if (k == 0 || n == 0)
        return;

    /// Gather this node's rows contiguously (the kernels want row-major contiguous input). The root reads the
    /// sample in place.
    VectorWithMemoryTracking<Float> gathered;
    const Float * pts = sample;
    if (!task.all_rows)
    {
        gathered.resize(n * d);
        for (size_t i = 0; i < n; ++i)
            memcpy(&gathered[i * d], sample + static_cast<size_t>(task.rows[i]) * d, d * sizeof(Float));
        pts = gathered.data();
    }

    pcg64 rng(task.seed);

    if (k >= n) /// fewer points than requested leaves: every point becomes a centroid
    {
        out_centroids.insert(out_centroids.end(), pts, pts + n * d);
        return;
    }

    if (k <= branching) /// base case: a single flat k-means with `k` clusters
    {
        auto centroids = kMeansLloyd(pts, n, d, k, params, rng);
        out_centroids.insert(out_centroids.end(), centroids.begin(), centroids.end());
        return;
    }

    const size_t br = branching;
    auto node = kMeansLloyd(pts, n, d, br, params, rng);

    VectorWithMemoryTracking<Float> ct(d * br);
    VectorWithMemoryTracking<Float> cnorm(br);
    packCentroids(node.data(), br, d, ct.data(), cnorm.data());

    VectorWithMemoryTracking<UInt32> assign(n);
    VectorWithMemoryTracking<Float> score(n);
    parallelRanges(n, std::max<size_t>(params.num_threads, 1), [&](size_t begin, size_t end, size_t)
    {
        assignRows(
            pts + begin * d, end - begin, d, ct.data(), cnorm.data(), br,
            assign.data() + begin, score.data() + begin);
    });

    VectorWithMemoryTracking<size_t> pop(br, 0);
    for (size_t i = 0; i < n; ++i)
        ++pop[assign[i]];

    /// If one child captured every point the split made no progress, and `apportion` would hand it all `k`
    /// leaves - the walk would then reproduce this node forever (all-identical points, say). Emit a flat
    /// k-means instead. Past this check every child is strictly smaller than its parent, bounding the walk.
    size_t non_empty = 0;
    for (size_t c = 0; c < br; ++c)
        non_empty += (pop[c] > 0);
    if (non_empty <= 1)
    {
        auto flat = kMeansLloyd(pts, n, d, k, params, rng);
        out_centroids.insert(out_centroids.end(), flat.begin(), flat.end());
        return;
    }

    auto leaves = apportion(pop, k);

    for (size_t c = 0; c < br; ++c)
    {
        if (leaves[c] == 0 || pop[c] == 0)
            continue;
        if (leaves[c] == 1) /// keep the node centroid itself as the single leaf
        {
            out_centroids.insert(out_centroids.end(), node.data() + c * d, node.data() + (c + 1) * d);
            continue;
        }

        TrainTask child;
        child.leaves = leaves[c];
        child.seed = mixSeed(task.seed, c);
        child.rows.reserve(pop[c]);
        /// Children carry indices into the WHOLE sample, so a gather is always one hop from the original
        /// buffer rather than a copy of a copy at every level.
        for (size_t i = 0; i < n; ++i)
            if (assign[i] == c)
                child.rows.push_back(task.all_rows ? static_cast<UInt32>(i) : task.rows[i]);
        out_children.push_back(std::move(child));
    }
}

/// Breadth-first walk of the training tree, appending the `k` leaf centroids to `out`. Not recursive: near
/// the root a few nodes hold most points so ROWS split across threads, deep down thousands of tiny NODES run
/// concurrently, and nesting the two would deadlock the pool - so each level picks exactly one regime.
void trainHierarchical(
    const Float * sample, size_t sample_rows, size_t d, size_t k, size_t branching,
    size_t iters, bool spherical, UInt64 seed, PaddedPODArray<Float> & out)
{
    if (k == 0 || sample_rows == 0)
        return;

    const size_t max_threads = getMaxTrainingThreads();

    TaskList level;
    {
        TrainTask root;
        root.all_rows = true;
        root.leaves = k;
        root.seed = mixSeed(seed, 0); /// same de-correlation as the reservoir RNG
        level.push_back(std::move(root));
    }

    while (!level.empty())
    {
        const size_t num_tasks = level.size();
        VectorWithMemoryTracking<VectorWithMemoryTracking<Float>> outs(num_tasks);
        VectorWithMemoryTracking<TaskList> kids(num_tasks);

        if (max_threads > 1 && num_tasks >= max_threads)
        {
            /// Enough independent nodes to fill the pool: one pooled task per node, each node serial inside.
            const KMeansParams params{iters, spherical, 1};
            ThreadPoolCallbackRunnerLocal<void> runner(getTrainingThreadPool(), ThreadName::MERGETREE_VECTOR_SIM_INDEX);
            for (size_t i = 0; i < num_tasks; ++i)
            {
                runner.enqueueAndKeepTrack([&, i]
                {
                    throwIfKilled();
                    processTask(level[i], sample, sample_rows, d, branching, params, outs[i], kids[i]);
                });
            }
            runner.waitForAllToFinishAndRethrowFirstError();
        }
        else
        {
            /// Too few nodes to fill the pool: walk them one at a time, parallelizing over rows instead.
            const KMeansParams params{iters, spherical, max_threads};
            for (size_t i = 0; i < num_tasks; ++i)
            {
                throwIfKilled();
                processTask(level[i], sample, sample_rows, d, branching, params, outs[i], kids[i]);
            }
        }

        /// Concatenated in task order, so the output never depends on completion order.
        for (size_t i = 0; i < num_tasks; ++i)
            out.insert(outs[i].data(), outs[i].data() + outs[i].size());

        TaskList next;
        for (size_t i = 0; i < num_tasks; ++i)
            for (auto & child : kids[i])
                next.push_back(std::move(child));
        level = std::move(next);
    }

    /// The `k >= n` shortcut emits raw sample points, which are not unit length; normalize the final set so
    /// every emitted centroid satisfies the spherical contract regardless of which path produced it.
    if (spherical && !out.empty())
        normalizeCentroids(out.data(), out.size() / d, d);
}

/// Aggregate state: a bounded reservoir of training vectors (uniform sample of the input stream).
struct HierarchicalKMeansData
{
    PaddedPODArray<Float> samples; /// flat, (samples.size() / dim) vectors
    UInt64 seen = 0;
    UInt32 dim = 0;

    /// `pcg32_fast` not `pcg64` because this generator is serialized: `IO/Operators_pcg_random.h` round-trips
    /// it, while pcg's stream operators cannot emit `pcg64`'s 128-bit state. The training RNG stays `pcg64`.
    pcg32_fast rng; /// seeded in create()

    /// Uniform in `[0, limit)`. One 32-bit draw does not cover a `limit` past 2^32, which `seen` reaches on a
    /// long stream, so widen with a second draw as `ReservoirSampler` does. The `limit == 0` guard is for the
    /// analyzer, which cannot see that every caller has just established the value is positive.
    UInt64 genRandom(UInt64 limit)
    {
        if (limit == 0)
            return 0;
        if (limit <= static_cast<UInt64>(pcg32_fast::max()))
            return rng() % limit;
        return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(pcg32_fast::max()) + 1ULL) + static_cast<UInt64>(rng())) % limit;
    }

    static constexpr size_t no_slot = std::numeric_limits<size_t>::max();

    /// Advance the reservoir for one incoming vector and return the row it should occupy, or `no_slot` when
    /// Algorithm R discards it. Deciding before copying is what keeps `add` allocation-free: a discarded row
    /// costs nothing, and a kept one is written straight into the reservoir rather than staged in a temporary.
    ///
    /// `dim == 0` is the "no rows yet" sentinel, so an empty input array would both make the state
    /// indistinguishable from empty and turn `samples.size() / dim` into a division by zero. Arrays are
    /// allowed to be empty, so this has to be rejected rather than assumed away.
    size_t reserveSlot(UInt32 d, UInt64 cap)
    {
        if (d == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: input vector must not be empty");

        if (dim == 0)
            dim = d;
        if (d != dim)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "hierarchicalKMeans: got a vector of size {} but expected {}", d, dim);

        ++seen;
        const UInt64 have = samples.size() / dim;
        if (have < cap)
        {
            samples.resize(samples.size() + d);
            return static_cast<size_t>(have);
        }

        const UInt64 j = genRandom(seen); /// Algorithm R reservoir sampling
        return j < cap ? static_cast<size_t>(j) : no_slot;
    }

    void addVector(const Float * v, UInt32 d, UInt64 cap)
    {
        const size_t slot = reserveSlot(d, cap);
        if (slot != no_slot)
            memcpy(&samples[slot * dim], v, d * sizeof(Float));
    }

    /// Same, but pulling coordinates through `read`, so a Float64 or BFloat16 column converts directly into
    /// the reservoir with no intermediate buffer.
    template <typename Reader>
    void addVectorFrom(Reader && read, UInt32 d, UInt64 cap)
    {
        const size_t slot = reserveSlot(d, cap);
        if (slot == no_slot)
            return;
        Float * dst = &samples[slot * dim];
        for (UInt32 j = 0; j < d; ++j)
            dst[j] = read(j);
    }

    /// Merge two reservoirs into a uniform sample of their union. Every branch must decide RANDOMLY which
    /// side a kept row comes from: fixing the per-side count to its expectation is neither uniform nor even
    /// order-independent (`cap = 1`, two one-row states -> `floor(1*1/2) = 0` always drops the left one).
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

        const UInt64 d = dim;
        const UInt64 have_a = samples.size() / d;
        const UInt64 have_b = other.samples.size() / d;

        /// Neither side overflows the reservoir: keeping everything IS the uniform sample.
        if (have_a + have_b <= cap)
        {
            samples.insert(other.samples.begin(), other.samples.end());
            seen += other.seen;
            return;
        }

        /// `other` never dropped a row, so replaying its rows through Algorithm R produces exactly the same
        /// distribution as if they had arrived on this stream in the first place.
        if (other.seen <= cap)
        {
            for (UInt64 i = 0; i < have_b; ++i)
                addVector(&other.samples[i * d], static_cast<UInt32>(d), cap);
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
            /// Bounded by the rows actually held rather than by `seen`. The two are equal here - a side with
            /// `seen <= cap` never dropped anything - but indexing `ours` by its own length is the version
            /// that stays in bounds if that ever stops holding.
            for (UInt64 i = 0; i < have_a; ++i)
                addVector(&ours[i * d], static_cast<UInt32>(d), cap);
            return;
        }

        /// Both sides overflowed, so each holds exactly `cap` rows. How many slots come from `other` is a
        /// HYPERGEOMETRIC draw over the combined stream - a per-slot coin gives the wrong (binomial) count,
        /// and picking rows with replacement can duplicate one. The urn simulation below is exact and O(cap).
        /// See https://en.wikipedia.org/wiki/Hypergeometric_distribution
        UInt64 take_b = 0;
        {
            UInt64 remaining_total = seen + other.seen;
            UInt64 remaining_b = other.seen;
            for (UInt64 i = 0; i < cap; ++i)
            {
                if (genRandom(remaining_total) < remaining_b)
                {
                    ++take_b;
                    --remaining_b;
                }
                --remaining_total;
            }
        }
        const UInt64 take_a = cap - take_b;

        /// Both sides then contribute WITHOUT replacement. Subsampling a uniform sample uniformly is itself
        /// uniform over the underlying stream, so the two halves compose into a uniform sample of the union.
        /// `take_a <= cap == have_a` and `take_b <= cap == have_b`, so neither side can be over-drawn.

        /// Partial Fisher-Yates over our own rows, moving the survivors to the front.
        for (UInt64 i = 0; i < take_a; ++i)
        {
            const UInt64 j = i + genRandom(have_a - i);
            if (j != i)
                for (UInt64 t = 0; t < d; ++t)
                    std::swap(samples[i * d + t], samples[j * d + t]);
        }
        samples.resize(take_a * d);

        /// Same for `other`, but it is const, so permute an index array rather than the rows.
        VectorWithMemoryTracking<UInt64> idx(have_b);
        std::iota(idx.begin(), idx.end(), 0);
        for (UInt64 i = 0; i < take_b; ++i)
        {
            const UInt64 j = i + genRandom(have_b - i);
            std::swap(idx[i], idx[j]);
            samples.insert(&other.samples[idx[i] * d], &other.samples[(idx[i] + 1) * d]);
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
                "(k, branching, max_iter, sample_cap, seed, spherical), got {}", params.size());

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
        spherical  = param(5, "spherical", 0) != 0;

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

        /// `add` reads the nested column as `ColumnFloat32`, so `Float32` is required exactly - accepting any float
        /// here would reinterpret e.g. `Float64` payload as `Float32` and silently train on garbage.
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

        /// Read the coordinate at `start + j` as Float32, whatever width the column actually holds.
        const IColumn & nested_col = array.getData();
        auto coord = [&](size_t j) -> Float
        {
            switch (nested_type)
            {
                case TypeIndex::Float32: return assert_cast<const ColumnFloat32 &>(nested_col).getData()[start + j];
                case TypeIndex::Float64: return static_cast<Float>(assert_cast<const ColumnFloat64 &>(nested_col).getData()[start + j]);
                default:                 return static_cast<Float>(assert_cast<const ColumnBFloat16 &>(nested_col).getData()[start + j]);
            }
        };

        /// One pass covering both input contracts.
        ///
        /// Non-finite coordinates are rejected because no comparison against NaN is ever true, so the
        /// assignment kernel would quietly collect those rows into cluster 0 and the trained centroids could
        /// come out non-finite. The rest of the vector-search stack treats them as invalid input the same way
        /// (`checkVectorIsSane` in `MergeTreeIndexVectorSimilarity.cpp`).
        ///
        /// A zero vector has no direction, so cosine against it is undefined. Under `spherical = 1` every
        /// centroid is meant to be a unit direction, so reject rather than let a zero-norm point drag a
        /// cluster mean toward the origin.
        const Float limit = coordinateLimit(length);
        double norm2 = 0;
        for (size_t j = 0; j < length; ++j)
        {
            const Float x = coord(j);
            if (!std::isfinite(x))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "hierarchicalKMeans: input vector must not contain non-finite values (NaN or Inf)");
            if (std::abs(x) > limit)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "hierarchicalKMeans: coordinate {} exceeds the largest magnitude the Float32 training "
                    "math can represent for dimension {} ({})", x, length, limit);
            norm2 += static_cast<double>(x) * static_cast<double>(x);
        }
        if (spherical && norm2 == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "hierarchicalKMeans: zero-norm vectors are not allowed with spherical = 1 "
                "(cosine is undefined for a vector with no direction)");

        /// Float32 keeps its memcpy; the other widths convert straight into the reservoir slot.
        if (nested_type == TypeIndex::Float32)
            data(place).addVector(
                &assert_cast<const ColumnFloat32 &>(nested_col).getData()[start], static_cast<UInt32>(length), sample_cap);
        else
            data(place).addVectorFrom(coord, static_cast<UInt32>(length), sample_cap);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs), sample_cap);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        const auto & d = data(place);
        writeBinary(d.dim, buf);
        writeBinary(d.seen, buf);
        writeVarUInt(d.samples.size(), buf);
        buf.write(reinterpret_cast<const char *>(d.samples.data()), d.samples.size() * sizeof(Float));

        /// The reservoir is only uniform if the PRNG keeps advancing across the serialization boundary. Without
        /// this a state that crosses a distributed merge resumes from a default-constructed generator, so every
        /// shard replays the same draws and later merges stop matching the pre-serialization behaviour.
        WriteBufferFromOwnString rng_buf;
        rng_buf << d.rng;
        writeStringBinary(rng_buf.str(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        auto & d = data(place);
        readBinary(d.dim, buf);
        readBinary(d.seen, buf);
        size_t n = 0;
        readVarUInt(n, buf);

        /// Guard the `dim > 0 whenever there are rows` invariant that the rest of the state relies on, rather
        /// than dividing by zero later on a corrupt or hostile state.
        if (n > 0 && (d.dim == 0 || n % d.dim != 0))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "hierarchicalKMeans: corrupt aggregate state ({} values for dimension {})", n, d.dim);

        /// States are user-transportable via `hierarchicalKMeansState`, so everything read above is untrusted.
        /// Re-establish the full reservoir invariant, not just an upper bound: `merge` reads `seen > cap` as
        /// proof that the side holds exactly `cap` rows and indexes on that basis, so a state claiming
        /// `seen = cap + 1` while storing one row would walk off the end of `samples`.
        const UInt64 have = d.dim ? n / d.dim : 0;
        const UInt64 expected = std::min<UInt64>(d.seen, sample_cap);
        if (have != expected)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "hierarchicalKMeans: aggregate state holds {} vectors, but seen = {} with sample_cap = {} "
                "requires exactly {}", have, d.seen, sample_cap, expected);

        d.samples.resize(n);
        buf.readStrict(reinterpret_cast<char *>(d.samples.data()), n * sizeof(Float));

        /// `add` enforces these on the way in, but a transported state bypasses `add` entirely, so both
        /// contracts have to be re-established before the payload can reach `kMeansLloyd`.
        const Float limit = d.dim ? coordinateLimit(d.dim) : 0;
        for (UInt64 i = 0; i < have; ++i)
        {
            double norm2 = 0;
            for (UInt64 j = 0; j < d.dim; ++j)
            {
                const Float x = d.samples[i * d.dim + j];
                if (!std::isfinite(x))
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "hierarchicalKMeans: aggregate state contains non-finite values (NaN or Inf)");
                if (std::abs(x) > limit)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "hierarchicalKMeans: aggregate state contains coordinate {}, above the largest "
                        "magnitude the Float32 training math can represent for dimension {} ({})",
                        x, d.dim, limit);
                norm2 += static_cast<double>(x) * static_cast<double>(x);
            }
            if (spherical && norm2 == 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "hierarchicalKMeans: aggregate state contains a zero-norm vector, which spherical = 1 "
                    "does not allow");
        }

        String rng_string;
        readStringBinary(rng_string, buf);
        ReadBufferFromString rng_buf(rng_string);
        rng_buf >> d.rng;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & d = data(place);
        auto & outer = assert_cast<ColumnArray &>(to);
        auto & inner = assert_cast<ColumnArray &>(outer.getData());
        auto & values = assert_cast<ColumnFloat32 &>(inner.getData()).getData();

        if (d.dim == 0) /// empty input -> empty array of centroids
        {
            outer.getOffsets().push_back(inner.getOffsets().size());
            return;
        }

        PaddedPODArray<Float> centroids;
        trainHierarchical(
            d.samples.data(), d.samples.size() / d.dim, d.dim, k, branching, max_iter, spherical, seed, centroids);

        size_t produced = centroids.size() / d.dim;
        for (size_t c = 0; c < produced; ++c)
        {
            values.insert(centroids.data() + c * d.dim, centroids.data() + (c + 1) * d.dim);
            inner.getOffsets().push_back(values.size());
        }
        outer.getOffsets().push_back(inner.getOffsets().size());
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
        "Trains up to k cluster centroids from the aggregated vectors using hierarchical k-means and returns "
        "them as Array(Array(Float32)). Fewer than k are returned only when the input has fewer than k rows, "
        "since a row can yield at most one centroid; repeated points still yield k. Distance is squared L2; "
        "pass `spherical = 1` to "
        "renormalize the centroids to unit length after each iteration, which makes the same centroids an exact "
        "cosine/inner-product quantizer.";
    FunctionDocumentation::Syntax syntax = "hierarchicalKMeans(k[, branching[, max_iter[, sample_cap[, seed[, spherical]]]]])(vec)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "Input vector to cluster.", {"Array(Float32)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
        {"An array of up to k centroids, capped by the number of input rows.", {"Array(Array(Float32))"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT length(hierarchicalKMeans(256)(vec)) FROM sample", ""}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    /// Order-dependent: the `k >= n` shortcut emits points in arrival order, and Algorithm R consumes RNG
    /// draws by stream position. Claiming otherwise lets `removeRedundantSorting` drop an upstream `ORDER BY`
    /// and silently change the trained centroids.
    AggregateFunctionProperties properties = { .is_order_dependent = true };
    factory.registerFunction("hierarchicalKMeans",
        {HierarchicalKMeansImpl::createAggregateFunctionHierarchicalKMeans, documentation, properties});
}

}
