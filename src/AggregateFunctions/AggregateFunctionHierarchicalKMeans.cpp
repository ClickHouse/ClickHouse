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
#include <limits>
#include <numeric>

/// `hierarchicalKMeans(k [, branching] [, max_iter] [, sample_cap] [, seed] [, spherical])(vec)`
///
/// Parametric aggregate that trains `k` cluster centroids from the aggregated vectors and returns them as
/// `Array(Array(Float32))` (k rows of `dim` floats). Intended to build the coarse quantizer for an IVF vector index:
///
///     INSERT INTO centroids
///     SELECT rowNumberInAllBlocks()::UInt32 AS cid, c
///     FROM (SELECT arrayJoin(hierarchicalKMeans(32768)(vec)) AS c FROM sample);
///
/// The table feeding it may hold anywhere from ~1-2M to billions of vectors: the aggregate keeps a bounded
/// RESERVOIR of `sample_cap` vectors (uniform sample), so memory is O(sample_cap * dim) regardless of input size.
/// A small table (< sample_cap) is kept in full; a billion-row table is uniformly downsampled. k-means centroids
/// depend on the data distribution, not the count, so a large sample yields essentially the same centroids as
/// training on all rows.
///
/// "Hierarchical" refers to the TRAINING algorithm, not the output: centroids are grown by a recursive
/// split (branch `branching` at each level), so per-point work is O(branching * log_branching(k)) instead of the
/// O(k) of flat Lloyd - this is what makes k in the tens of thousands trainable. The OUTPUT is the flat set of
/// `k` leaf centroids (the tree is scaffolding, discarded). Leaves are allocated to children proportional to
/// population, which keeps final cell sizes balanced (anti-skew).
///
/// Distance is squared L2 throughout, matching `assignCentroid`. With `spherical = 1` the centroids are
/// renormalized to unit length after every Lloyd update, which turns the same L2 argmin into an exact
/// cosine/inner-product argmin (with `||c|| = 1` the `||c||^2` term is constant, so `argmin ||x-c||^2 ==
/// argmax x.c == argmax cos(x, c)` for any `||x||`). Use it when the search path ranks by `cosineDistance`
/// and the vectors are not L2-normalized at ingest; if they are normalized, plain L2 is already equivalent.
///
/// Training is parallelized over the thread pool shared with vector similarity index builds
/// (`max_build_vector_similarity_index_thread_pool_size`), and the hot argmin/accumulate loops are
/// runtime-dispatched to the widest SIMD the CPU supports.

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

DECLARE_MULTITARGET_CODE(

/// For every point, find `argmin_c (||c||^2 - 2 x.c)`, which is `argmin_c ||x - c||^2` with the constant
/// `||x||^2` dropped.
///
/// `ct` is COLUMN-major (`ct[j * k + c]` = coordinate `j` of centroid `c`). That layout is what makes this
/// vectorize: the inner loop over centroids walks `k` contiguous floats scaled by a broadcast `x[j]`, and the
/// `k` accumulators stay in registers for the whole `d` loop. In the hierarchical path `k == branching` (16 by
/// default), so a tile is two AVX2 FMAs per dimension with no accumulator traffic at all.
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
    ThreadPoolCallbackRunnerLocal<void> runner(getTrainingThreadPool(), ThreadName::HIERARCHICAL_KMEANS);
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

/// Renormalize centroids to unit length. With `||c|| = 1` the `||c||^2` term of the argmin is a constant, so
/// the same L2 kernels become an exact cosine/inner-product argmin.
///
/// The `||c|| = 1` guarantee has to be absolute - a single zero-norm centroid would leave `assignCentroid`
/// ranking against a direction-less centroid and quietly break the cosine equivalence the whole mode rests
/// on. Zero-norm INPUT vectors are rejected at `add` time (cosine is undefined for them), so the only way to
/// reach a zero here is exact cancellation of a cluster mean, e.g. a cluster of perfectly antipodal points.
/// Substitute a fixed unit vector in that case: the direction is arbitrary, but no arbitrary choice is worse
/// than a centroid that satisfies neither metric.
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

    /// Per-thread accumulation buffers, so the mean update is parallel too - it is only 1/k of the assignment
    /// work, but leaving it serial would cap the whole speedup by Amdahl at large thread counts. The buffers
    /// are `k * d` doubles EACH, so cap the threads for this step by a memory budget rather than blowing up
    /// on a large flat `k`.
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

/// Split `k` leaves across the `B` children, proportional to population (largest-remainder).
///
/// Two constraints decide the shape, and getting either wrong silently loses centroids - the caller has no
/// way to emit a leaf it was promised but cannot train:
///   * a child with NO points gets NO leaves. k-means reseeds empty clusters while iterating, but the final
///     bucketing pass can still leave a centroid owning nothing, and handing that child a leaf (as an
///     unconditional `leaves(B, 1)` does) means `processTask` skips it and the leaf is gone.
///   * no child gets more leaves than it has points, since it can only ever emit one centroid per point.
/// Whatever those two rules displace is redistributed to children that still have headroom, so the total is
/// exactly `k` whenever the node has at least `k` points at all.
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

    /// If one child captured every point the split made no progress, and since `apportion` would then hand
    /// that child all `k` leaves the recursion would reproduce this exact node forever. It happens on
    /// degenerate input - a node whose points are all identical, say, where every centroid collapses onto the
    /// same value. Emit a flat k-means here instead; `kMeansLloyd` clamps to `min(k, n)` and always
    /// terminates. Past this check every child is strictly smaller than its parent, which is what bounds the
    /// recursion.
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

/// Level-by-level (breadth-first) walk of the training tree, appending the `k` leaf centroids to `out`.
///
/// Breadth-first rather than recursive because the two levels of parallelism live in different regimes and
/// must not nest: near the root there are few nodes but each holds most of the points, so the win is
/// splitting ROWS across threads; deep in the tree there are thousands of tiny nodes, so the win is running
/// NODES concurrently. Nesting them (a pooled task waiting on sub-tasks from the same pool) can deadlock, so
/// each level picks exactly one regime.
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
            ThreadPoolCallbackRunnerLocal<void> runner(getTrainingThreadPool(), ThreadName::HIERARCHICAL_KMEANS);
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

    /// `pcg32_fast` rather than `pcg64` specifically because this generator crosses the serialization
    /// boundary: `IO/Operators_pcg_random.h` can round-trip it, while `pcg64` carries a 128-bit state that
    /// pcg's own stream operators cannot emit here (`unsigned __int128` has no `ostream` overload). The
    /// training RNG in `insertResultInto` is never serialized and stays `pcg64`.
    pcg32_fast rng; /// seeded in create()

    /// Uniform in `[0, limit)`. One 32-bit draw does not cover a `limit` past 2^32, which `seen` reaches on a
    /// large enough stream, so widen with a second draw exactly as `ReservoirSampler` does.
    UInt64 genRandom(UInt64 limit)
    {
        if (limit <= static_cast<UInt64>(pcg32_fast::max()))
            return rng() % limit;
        return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(pcg32_fast::max()) + 1ULL) + static_cast<UInt64>(rng())) % limit;
    }

    /// Uniform in [0, 1).
    double randomDouble()
    {
        return static_cast<double>(rng()) / (static_cast<double>(pcg32_fast::max()) + 1.0);
    }

    void addVector(const Float * v, UInt32 d, UInt64 cap)
    {
        /// `dim == 0` is the "no rows yet" sentinel, so an empty input array would both make the state
        /// indistinguishable from empty and turn `samples.size() / dim` below into a division by zero.
        /// `Array(Float32)` permits empty arrays, so this has to be rejected rather than assumed away.
        if (d == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: input vector must not be empty");

        if (dim == 0)
            dim = d;
        if (d != dim)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "hierarchicalKMeans: got a vector of size {} but expected {}", d, dim);

        ++seen;
        UInt64 have = samples.size() / dim;
        if (have < cap)
        {
            samples.insert(v, v + d);
        }
        else
        {
            UInt64 j = genRandom(seen); /// Algorithm R reservoir sampling
            if (j < cap)
                memcpy(&samples[j * dim], v, d * sizeof(Float));
        }
    }

    /// Merge two reservoirs into a sample of their union.
    ///
    /// Every branch below has to make a RANDOM decision about which side a kept row comes from. An earlier
    /// version fixed the per-side counts to their expectation (`cap * seen / total_seen`), which is not a
    /// uniform merge and is not even order-independent: with `cap = 1` and two one-row states the split is
    /// `floor(1 * 1 / 2) = 0`, so the left sample was always dropped and the right one always kept.
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
            const UInt64 ours_seen = seen;
            seen = other.seen;
            for (UInt64 i = 0; i < ours_seen; ++i)
                addVector(&ours[i * d], static_cast<UInt32>(d), cap);
            return;
        }

        /// Both sides were downsampled, so both hold exactly `cap` rows. Replace each slot with a row drawn
        /// from `other` with probability equal to `other`'s share of the combined stream. The choice is made
        /// per slot rather than as a fixed count, which is what keeps the merge order-independent.
        const double p = static_cast<double>(other.seen) / (static_cast<double>(seen) + static_cast<double>(other.seen));
        for (UInt64 i = 0; i < have_a; ++i)
        {
            if (randomDouble() < p)
            {
                const UInt64 j = genRandom(have_b);
                memcpy(&samples[i * d], &other.samples[j * d], d * sizeof(Float));
            }
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

public:
    AggregateFunctionHierarchicalKMeans(const DataTypes & args, const Array & params)
        : IAggregateFunctionDataHelper<HierarchicalKMeansData, AggregateFunctionHierarchicalKMeans>(args, params, createResultType())
    {
        if (params.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function hierarchicalKMeans requires at least the parameter k");

        k          = params[0].safeGet<UInt64>();
        branching  = params.size() > 1 ? params[1].safeGet<UInt64>() : 16;
        max_iter   = params.size() > 2 ? params[2].safeGet<UInt64>() : 20;
        sample_cap = params.size() > 3 ? params[3].safeGet<UInt64>() : 1'000'000;
        seed       = params.size() > 4 ? params[4].safeGet<UInt64>() : 0;
        spherical  = params.size() > 5 && params[5].safeGet<UInt64>() != 0;

        if (k == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: k must be greater than 0");
        branching = std::max<size_t>(branching, 2);
        max_iter = std::max<size_t>(max_iter, 1);
        if (sample_cap == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: sample_cap must be greater than 0");

        /// `add` reads the nested column as `ColumnFloat32`, so `Float32` is required exactly - accepting any float
        /// here would reinterpret e.g. `Float64` payload as `Float32` and silently train on garbage.
        const auto * array_type = typeid_cast<const DataTypeArray *>(args[0].get());
        if (!array_type || !WhichDataType(array_type->getNestedType()).isFloat32())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function hierarchicalKMeans requires an Array(Float32) argument "
                "(CAST an Array(Float64) or Array(BFloat16) column to Array(Float32) first)");
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
        const auto & nested = assert_cast<const ColumnFloat32 &>(array.getData()).getData();
        const auto & offsets = array.getOffsets();
        size_t start = row_num ? offsets[row_num - 1] : 0;
        size_t length = offsets[row_num] - start;

        /// A zero vector has no direction, so cosine against it is undefined. Under `spherical = 1` the whole
        /// point is that every centroid is a unit direction, so reject the input rather than let a zero-norm
        /// point silently drag a cluster mean toward the origin.
        if (spherical)
        {
            double norm2 = 0;
            for (size_t j = 0; j < length; ++j)
                norm2 += static_cast<double>(nested[start + j]) * static_cast<double>(nested[start + j]);
            if (norm2 == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "hierarchicalKMeans: zero-norm vectors are not allowed with spherical = 1 "
                    "(cosine is undefined for a vector with no direction)");
        }

        data(place).addVector(&nested[start], static_cast<UInt32>(length), sample_cap);
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

        d.samples.resize(n);
        buf.readStrict(reinterpret_cast<char *>(d.samples.data()), n * sizeof(Float));

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
        "Trains k cluster centroids from the aggregated vectors using hierarchical k-means and returns them as "
        "Array(Array(Float32)). Distance is squared L2; pass `spherical = 1` to renormalize the centroids to unit "
        "length after each iteration, which makes the same centroids an exact cosine/inner-product quantizer.";
    FunctionDocumentation::Syntax syntax = "hierarchicalKMeans(k[, branching[, max_iter[, sample_cap[, seed[, spherical]]]]])(vec)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "Input vector to cluster.", {"Array(Float32)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"An array of k centroids.", {"Array(Array(Float32))"}};
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
