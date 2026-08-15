/** hash_join_bandwidth_model: a theoretical model comparing Radix Partitioned Hash Join (RPHJ)
  * against Non-Partitioned Hash Join (NPHJ), parameterized by memory bandwidth terms measured on
  * this machine. To avoid deviations between the model and reality, every model term is measured
  * by running the SAME ClickHouse code that executes in the real joins (including its software
  * prefetching, `RowRef` machinery, stored-block handling and output materialization):
  *
  *   - memcpy bandwidth  B_cpy       : baseline sequential copy (block squashing via
  *                                     `insertRangeFrom`); reported for reference.
  *   - scatter bandwidth B_scatter(P): one single-pass call of the radix join's own partitioning
  *                                     code (scatterSide: per-worker histograms -> a fused
  *                                     parallel prefix sum + one exact allocation per partition
  *                                     -> column-major direct placement through batch-scoped
  *                                     2-byte partition ids, dropping consumed input eagerly,
  *                                     with software write-combining and non-temporal stores at
  *                                     fanout >= 256), swept over the fanout P to expose the
  *                                     fanout cliff; a single pass covers any partition count up
  *                                     to F_max, refine (multi-pass) passes handle the rest.
  *   - t_build_np(S)  : ns/row of the real `ConcurrentHashJoin` build phase (concurrent
  *                      `addBlockToJoin` with its internal hash/selector dispatch into per-slot
  *                      two-level maps, plus the `onBuildPhaseFinish` bucket merge), as a
  *                      function of total hash table byte size S.
  *   - t_build_rp(S)  : ns/row of per-thread private real `HashJoin::addBlockToJoin` — the
  *                      radix join's per-partition build — as a function of per-table size.
  *   - t_pg_np(S)     : ns/row of the real `ConcurrentHashJoin::joinBlock` from all T threads
  *                      against the shared merged map: probe, gather and output-Block
  *                      materialization fused, exactly as production runs them.
  *   - t_pg_rp(S)     : ns/row of per-thread private `HashJoin::joinBlock` — the radix join's
  *                      per-partition probe+gather.
  *   - gather         : standalone gather term (output Block built via a devirtualized per-row
  *                      copy by RowRef + `IColumn::replicate`, dropped in the timed region),
  *                      swept over the stored-build-side working set; reported for reference,
  *                      not used by the crossover model (production fuses gather into joinBlock).
  *
  * All kernels run multi-threaded on T threads, include memory allocation cost, and reuse no
  * memory across timed iterations except the immutable input blocks. Per-row times are wall
  * seconds * 1e9 / total rows over all threads, interpolated log-linearly in S between sweep
  * points and clamped at the ends.
  *
  * Model. Build side N_b rows of width w_b = 8 * (1 + build payload columns); probe side N_p rows
  * of width w_p; D distinct build keys; S(D) = byte size of `HashMap<UInt64, UInt64>` holding D
  * keys (exact grower emulation: load factor 0.5, growth x4 up to 2^23 cells, then x2); T threads.
  *
  *   T_NP = N_b * t_build_np(S) + N_p * t_pg_np(S)
  *
  * RPHJ partitions BOTH sides by key hash into P* partitions. Each partition's `HashJoin` is
  * `reserve()`'d from the scatter histogram's exact per-partition row count (no rehash growth;
  * see `RadixHashJoinBench::build`), so P* is the smallest power of two such that the *reserved*
  * per-partition table plus that partition's share of build rows fits the private-cache budget
  * C = L2, bumped up to at least pow2(T) for parallelism and capped by --max-partitions:
  *
  *   P* = smallest power of two with htBytesReserved(D/P*) + (N_b/P*)*w_b <= C = L2
  *
  * partitioning runs in n_pass scatter passes, each with fanout at most F_max - the largest
  * fanout still sustaining >= 80% of peak scatter bandwidth in a contiguous prefix of the sweep,
  * clamped by the SWWC implementation's compile-time memory-correctness ceiling
  * (`MAX_FANOUT_PER_PASS`) and an L2-derived cap - n_pass = ceil(log2(P*) / log2(F_max)):
  *
  *   T_RP = n_pass * (N_b*w_b + N_p*w_p) / B_scatter(per-pass fanout)
  *        + N_b * t_build_rp(S/P*) + N_p * t_pg_rp(S/P*)
  *
  * For n_pass == 2, the scatter term instead uses a directly measured 2-pass bandwidth when
  * available, since it already captures the refine path's single-threaded-per-group behavior
  * and the frees between passes instead of extrapolating from a single-pass number.
  *
  * Crossover condition (RPHJ wins iff):
  *
  *   N_b * [t_build_np(S) - t_build_rp(S/P*)] + N_p * [t_pg_np(S) - t_pg_rp(S/P*)]
  *   >  n_pass * (N_b*w_b + N_p*w_p) / B_scatter(f)
  *
  * which makes the regimes explicit:
  *   - input size: the left side is ~0 while S fits in cache -> NPHJ wins for small inputs;
  *   - key space:  duplicate-heavy keys (small D) keep S cache-resident regardless of N_b ->
  *     RPHJ rarely wins;
  *   - partition count: P* grows with S; once P* > F_max more scatter passes are needed,
  *     multiplying the partitioning cost and pushing the crossover out;
  *   - payload width: larger w inflates the scatter cost linearly but also the probe+gather delta.
  *
  * With duplicate-free build sides (as the unique-keys regime and the RP/NP sweeps use),
  * `onBuildPhaseFinish` promotes join strictness `All` to `RightAny`, so every timed probe in
  * this program runs the promoted point-lookup path and emits exactly one row per matching probe
  * row; the dup-key grid regimes reuse those same curves and are therefore labeled as optimistic
  * lower bounds, not predictions of a real INNER ALL join walking `RowRefList` chains over
  * duplicate keys. Join teardown (hash table and stored-block destruction) is timed as a
  * separate phase for both competitors, mirroring pipeline destruction after the last output
  * block in a real query, and is reported but not added to either competitor's total. The RPHJ
  * build side is a materialized copy of the scattered input, while `ConcurrentHashJoin` stores
  * zero-copy block references plus per-block selectors - an inherent memory-shape asymmetry
  * between the two designs that this model does not otherwise account for.
  *
  * The program measures the terms, prints the model constants, evaluates the model over a grid
  * of (N_b, N_p/N_b, key-space regime), prints the crossover summary, and finally (unless --quick)
  * validates the model by running real multi-threaded INNER joins at points near the predicted
  * crossover. The two competitors implement a common `IJoinBench` interface driven by the driver:
  *   - NPHJ is the real ClickHouse `ConcurrentHashJoin` (`parallel_hash`), used as-is through the
  *     `IJoin` interface (concurrent `addBlockToJoin`, `onBuildPhaseFinish` bucket merge,
  *     unpartitioned shared-map probe via `joinBlock`);
  *   - RPHJ is multi-pass radix partitioning (the same scatterSide code the scatter kernel
  *     measures) plus one real ClickHouse `HashJoin` per partition, built and probed
  *     single-threaded per partition through the same `IJoin` interface.
  * All phases run on the ClickHouse thread pool (`ThreadPoolImpl`, threads carry `ThreadStatus`
  * for efficient memory tracking); the binary uses jemalloc via `clickhouse_new_delete`.
  */

#include "config.h"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

#include <pcg_random.hpp>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <base/types.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/getNumberOfCPUCoresToUse.h>

#include "concurrent_hash_join_bench.h"
#include "hash_join_bench.h"
#include "radix_hash_join_bench.h"

using namespace DB;
using namespace DB::JoinBench;

namespace
{

static_assert(sizeof(HashMapCell<UInt64, UInt64, DefaultHash<UInt64>>) == 16);

/// Deterministic bijection on [0, n): a bijective mix on the covering power-of-two domain,
/// cycle-walked back into [0, n). Used to shuffle insertion/probe order over a sequential
/// key space (key values stay dense 0..n-1, like auto-increment ids).
UInt64 shuffledIndex(UInt64 x, UInt64 n)
{
    if (n <= 1)
        return 0;
    const UInt32 k = 64 - static_cast<UInt32>(std::countl_zero(n - 1));
    const UInt64 mask = (k >= 64) ? ~UInt64(0) : ((UInt64(1) << k) - 1);
    const UInt32 s = k / 2 + 1;
    UInt64 y = x;
    do
    {
        y ^= y >> s;
        y = (y * 0x9E3779B97F4A7C15ULL) & mask;
        y ^= y >> s;
        y = (y * 0xC2B2AE3D27D4EB4FULL) & mask;
    } while (y >= n);
    return y;
}

UInt64 packRowRef(size_t block, size_t row)
{
    return (static_cast<UInt64>(block) << 32) | static_cast<UInt64>(row);
}

size_t refBlock(UInt64 ref) { return ref >> 32; }
size_t refRow(UInt64 ref) { return ref & 0xFFFFFFFFULL; }

/// Keyspaces of different generator "slots" must not overlap.
constexpr UInt64 KEY_DOMAIN_STRIDE = 1ULL << 44;


struct Config
{
    size_t threads = 0;
    size_t build_payload_columns = 1;
    size_t probe_payload_columns = 1;
    size_t tuples = 1ULL << 27;
    double hit_rate = 1.0;
    size_t max_partitions = 16384;
    size_t max_table_bytes = 256ULL << 20;
    size_t gather_bytes = 4ULL << 30;
    size_t validation_max_rows = 1ULL << 26;
    size_t runs = 3;
    bool quick = false;
    bool verify = false;
    UInt64 seed = 0x8899AABBCCDDEEFFULL;

    /// Which real join algorithm(s) to build+drive in --join-nb and the validation joins; lets
    /// a debugging session iterate on just one side (e.g. NPHJ alone, to compare its production
    /// ProfileEvents against a real query) without paying for the other's construction and run.
    bool run_nphj = true;
    bool run_rphj = true;

    size_t buildRowWidth() const { return 8 * (1 + build_payload_columns); }
    size_t probeRowWidth() const { return 8 * (1 + probe_payload_columns); }
};


struct CacheInfo
{
    size_t l1d = 32ULL << 10;
    size_t l2 = 1ULL << 20;
    size_t llc = 32ULL << 20;
    bool detected = false;
};

size_t parseCacheSize(std::string s)
{
    while (!s.empty() && (s.back() == '\n' || s.back() == ' '))
        s.pop_back();
    if (s.empty())
        return 0;
    size_t multiplier = 1;
    char suffix = s.back();
    if (suffix == 'K' || suffix == 'k')
        multiplier = 1ULL << 10;
    else if (suffix == 'M' || suffix == 'm')
        multiplier = 1ULL << 20;
    else if (suffix == 'G' || suffix == 'g')
        multiplier = 1ULL << 30;
    if (multiplier != 1)
        s.pop_back();
    return static_cast<size_t>(std::stoull(s)) * multiplier;
}

std::string readSysfsLine(const std::filesystem::path & path)
{
    std::ifstream in(path);
    std::string line;
    std::getline(in, line);
    return line;
}

CacheInfo detectCaches()
{
    CacheInfo info;
    namespace fs = std::filesystem;

    try
    {
        int max_level = 0;
        /// (level, shared_cpu_list) -> size; used to count distinct LLC instances.
        std::map<std::pair<int, std::string>, size_t> instances;

        for (const auto & cpu_entry : fs::directory_iterator("/sys/devices/system/cpu"))
        {
            const std::string name = cpu_entry.path().filename().string();
            if (name.size() < 4 || !name.starts_with("cpu") || !isdigit(static_cast<unsigned char>(name[3])))
                continue;

            fs::path cache_dir = cpu_entry.path() / "cache";
            if (!fs::exists(cache_dir))
                continue;

            for (const auto & idx_entry : fs::directory_iterator(cache_dir))
            {
                if (!idx_entry.path().filename().string().starts_with("index"))
                    continue;

                const std::string type = readSysfsLine(idx_entry.path() / "type");
                if (type == "Instruction")
                    continue;

                const int level = std::stoi(readSysfsLine(idx_entry.path() / "level"));
                const size_t size = parseCacheSize(readSysfsLine(idx_entry.path() / "size"));
                const std::string shared = readSysfsLine(idx_entry.path() / "shared_cpu_list");
                if (size == 0)
                    continue;

                if (name == "cpu0" && level == 1)
                    info.l1d = size;
                if (name == "cpu0" && level == 2)
                    info.l2 = size;

                max_level = std::max(max_level, level);
                instances[{level, shared}] = size;
            }
        }

        if (max_level > 0)
        {
            size_t llc_total = 0;
            for (const auto & [key, size] : instances)
                if (key.first == max_level)
                    llc_total += size;
            if (llc_total > 0)
            {
                info.llc = llc_total;
                info.detected = true;
            }
        }
    }
    catch (...) /// NOLINT(bugprone-empty-catch)
    {
        /// Fall back to defaults; the values are overridable from the command line.
    }

    return info;
}


template <typename F>
double medianTime(size_t runs, F && once)
{
    once(); /// warmup
    std::vector<double> times(runs);
    for (auto & t : times)
        t = once();
    std::sort(times.begin(), times.end());
    return times[times.size() / 2];
}


/// Input generation: vector of Blocks with one UInt64 key column and a number of UInt64
/// payload columns, generated in parallel. Block b is owned by thread b % threads;
/// local_row passed to the generator is the row index within the owning thread's stream.
using KeyGenerator = std::function<UInt64(size_t block_idx, size_t row_in_block, size_t local_row, pcg64_fast & rng)>;

std::vector<Block> generateBlocks(
    WorkerPool & pool,
    size_t rows,
    size_t payload_columns,
    const std::string & name_prefix,
    const KeyGenerator & keygen,
    UInt64 seed)
{
    const size_t threads = pool.size();
    const size_t num_blocks = (rows + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE;
    std::vector<Block> blocks(num_blocks);
    auto type = std::make_shared<DataTypeUInt64>();

    pool.run([&](size_t tid)
    {
        pcg64_fast rng(seed * 0x9E3779B1ULL + tid);
        for (size_t b = tid; b < num_blocks; b += threads)
        {
            const size_t block_rows = std::min<size_t>(DEFAULT_BLOCK_SIZE, rows - b * DEFAULT_BLOCK_SIZE);
            const size_t local_base = (b / threads) * DEFAULT_BLOCK_SIZE;

            Block block;

            auto key_col = ColumnUInt64::create(block_rows);
            auto & key_data = key_col->getData();
            for (size_t i = 0; i < block_rows; ++i)
                key_data[i] = keygen(b, i, local_base + i, rng);
            block.insert(ColumnWithTypeAndName(std::move(key_col), type, name_prefix + "key"));

            for (size_t c = 0; c < payload_columns; ++c)
            {
                auto col = ColumnUInt64::create(block_rows);
                auto & data = col->getData();
                for (size_t i = 0; i < block_rows; ++i)
                    data[i] = b * DEFAULT_BLOCK_SIZE + i;
                block.insert(ColumnWithTypeAndName(std::move(col), type, name_prefix + "p" + std::to_string(c)));
            }

            blocks[b] = std::move(block);
        }
    });

    return blocks;
}

size_t totalRows(const std::vector<Block> & blocks)
{
    size_t rows = 0;
    for (const auto & b : blocks)
        rows += b.rows();
    return rows;
}

/// Build-side keys: the sequential key space [0, n), each value exactly once, in shuffled order.
KeyGenerator uniqueKeys(size_t n)
{
    return [n](size_t block_idx, size_t row_in_block, size_t /*local_row*/, pcg64_fast &)
    {
        return shuffledIndex(block_idx * DEFAULT_BLOCK_SIZE + row_in_block, n);
    };
}

/// Build-side keys: one global sequential keyspace of `distinct` values, fully covered (in
/// shuffled order) by the first `distinct` rows, random duplicates afterwards.
KeyGenerator globalDomainKeys(size_t distinct)
{
    return [distinct](size_t block_idx, size_t row_in_block, size_t /*local_row*/, pcg64_fast & rng)
    {
        const UInt64 global_row = block_idx * DEFAULT_BLOCK_SIZE + row_in_block;
        return global_row < distinct ? shuffledIndex(global_row, distinct) : rng() % distinct;
    };
}

/// Probe-side keys against a keyspace of `distinct` values: hits with probability hit_rate,
/// misses drawn from a disjoint keyspace.
KeyGenerator probeKeys(size_t distinct, size_t threads, double hit_rate, bool per_thread_domain)
{
    const UInt64 hit_threshold = hit_rate >= 1.0
        ? std::numeric_limits<UInt64>::max()
        : static_cast<UInt64>(hit_rate * static_cast<double>(std::numeric_limits<UInt64>::max()));

    return [distinct, threads, hit_threshold, per_thread_domain](size_t block_idx, size_t /*row_in_block*/, size_t /*local_row*/, pcg64_fast & rng)
    {
        const UInt64 offset = per_thread_domain ? (block_idx % threads) * KEY_DOMAIN_STRIDE : 0;
        const bool hit = rng() <= hit_threshold;
        const UInt64 raw = rng() % distinct + (hit ? 0 : distinct);
        return offset + raw;
    };
}

/// Probe-side keys for the join runs: an exact permutation of the build key space [0, n_b) —
/// with N_p = r * n_b every build key appears exactly r times (+-1 when not divisible), in
/// shuffled order. With hit_rate < 1, a random subset of rows is redirected to the disjoint
/// range [n_b, 2*n_b) instead (no longer an exact permutation).
KeyGenerator probePermutationKeys(size_t n_b, size_t n_p, double hit_rate)
{
    const UInt64 hit_threshold = hit_rate >= 1.0
        ? std::numeric_limits<UInt64>::max()
        : static_cast<UInt64>(hit_rate * static_cast<double>(std::numeric_limits<UInt64>::max()));

    return [n_b, n_p, hit_threshold](size_t block_idx, size_t row_in_block, size_t /*local_row*/, pcg64_fast & rng)
    {
        const UInt64 global_row = block_idx * DEFAULT_BLOCK_SIZE + row_in_block;
        const UInt64 key = shuffledIndex(global_row, n_p) % n_b;
        const bool hit = hit_threshold == std::numeric_limits<UInt64>::max() || rng() <= hit_threshold;
        return hit ? key : key + n_b;
    };
}


std::string formatBytes(double bytes)
{
    const char * units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    size_t unit = 0;
    while (bytes >= 1024.0 && unit < 4)
    {
        bytes /= 1024.0;
        ++unit;
    }
    return fmt::format("{:.1f} {}", bytes, units[unit]);
}


/// Piecewise log-linear interpolation of ns/row over a size sweep.
struct Curve
{
    /// (bytes, ns_per_row), sorted by bytes.
    std::vector<std::pair<double, double>> points;

    double at(double bytes) const
    {
        chassert(!points.empty());
        if (bytes <= points.front().first)
            return points.front().second;
        if (bytes >= points.back().first)
            return points.back().second;
        for (size_t i = 1; i < points.size(); ++i)
        {
            if (bytes <= points[i].first)
            {
                const double x0 = std::log2(points[i - 1].first);
                const double x1 = std::log2(points[i].first);
                const double x = std::log2(bytes);
                const double f = (x - x0) / (x1 - x0);
                return points[i - 1].second + f * (points[i].second - points[i - 1].second);
            }
        }
        return points.back().second;
    }
};


/// ---------------------------------------------------------------------------------------------
/// Kernel 1: memcpy baseline. Each thread squashes its share of input blocks into freshly
/// allocated columns via insertRangeFrom, then the columns are dropped.
/// ---------------------------------------------------------------------------------------------
double runMemcpyKernel(const Config & cfg, WorkerPool & pool, const std::vector<Block> & blocks)
{
    const size_t threads = cfg.threads;
    const size_t rows = totalRows(blocks);
    const size_t row_width = 8 * blocks.front().columns();

    double seconds = medianTime(cfg.runs, [&]
    {
        return pool.run([&](size_t tid)
        {
            size_t my_rows = 0;
            for (size_t b = tid; b < blocks.size(); b += threads)
                my_rows += blocks[b].rows();

            MutableColumns dst;
            for (size_t j = 0; j < blocks.front().columns(); ++j)
            {
                dst.emplace_back(blocks.front().getByPosition(j).column->cloneEmpty());
                dst.back()->reserve(my_rows);
            }

            for (size_t b = tid; b < blocks.size(); b += threads)
                for (size_t j = 0; j < dst.size(); ++j)
                    dst[j]->insertRangeFrom(*blocks[b].getByPosition(j).column, 0, blocks[b].rows());

            g_sink += dst.front()->size();
            /// dst deallocated here, inside the timed region.
        });
    });

    return static_cast<double>(rows) * static_cast<double>(row_width) / seconds;
}


/// ---------------------------------------------------------------------------------------------
/// Kernel 2: scatter. One single-pass call of the radix join's own partitioning code
/// (scatterSide: histogram + prefix sum + direct placement through batch-scoped partition ids),
/// with all output freshly allocated and dropped inside the timed region. Sweep the fanout.
/// Note the input blocks are shared and outlive the call, so the scatter's eager per-batch
/// input drops release only this call's references - the kernel times the drops, not the frees.
/// ---------------------------------------------------------------------------------------------
struct ScatterPoint
{
    size_t fanout;
    double bytes_per_sec;
};

/// Result of the scatter sweep: per-fanout single-pass bandwidth points, plus (when measured) the
/// effective useful-bytes/s of one true 2-pass scatter, used directly by predict() instead of
/// extrapolating a 2-pass cost from single-pass numbers.
struct ScatterMeasurement
{
    std::vector<ScatterPoint> points;
    double two_pass_eff_bytes_per_sec = 0; /// effective useful-bytes/s of a full 2-pass scatter (0 = not measured)
};

ScatterMeasurement runScatterKernel(const Config & cfg, WorkerPool & pool, const std::vector<Block> & blocks)
{
    const size_t rows = totalRows(blocks);
    const size_t row_width = 8 * blocks.front().columns();

    ScatterMeasurement result;

    fmt::print("\n=== scatter bandwidth (fanout sweep, {} input, radix join's scatterSide) ===\n",
        formatBytes(static_cast<double>(rows) * static_cast<double>(row_width)));
    fmt::print("{:>10}{:>14}\n", "fanout", "GB/s");

    /// Explicit fanout list rather than a pure geometric *4 step: 256 (SWWC_MIN_FANOUT, where
    /// the scatter switches from the direct to the SWWC implementation) is inserted as a
    /// measured point so predict()/scatterBytesPerSec never interpolates log-linearly across
    /// that implementation discontinuity.
    std::vector<size_t> fanouts;
    for (size_t fanout = 2; fanout <= cfg.max_partitions; fanout *= 4)
        fanouts.push_back(fanout);
    if (cfg.max_partitions >= 256)
        fanouts.push_back(256);
    std::sort(fanouts.begin(), fanouts.end());
    fanouts.erase(std::unique(fanouts.begin(), fanouts.end()), fanouts.end());

    /// Measures scatterSide once, including a parallel free of the resulting partitions inside
    /// the timed region (mirroring a real radix join, which frees partitions from the same
    /// worker pool that produced them) instead of relying on the implicit single-threaded
    /// destruction that would otherwise run when `partitions` goes out of scope.
    auto measure_once = [&](const std::vector<size_t> & pass_bits)
    {
        return medianTime(cfg.runs, [&]
        {
            Stopwatch watch;
            {
                auto partitions = scatterSide(pool, blocks, pass_bits);
                g_sink += partitions.size();
                pool.run([&](size_t tid)
                {
                    for (size_t p = tid; p < partitions.size(); p += cfg.threads)
                        partitions[p].clear();
                });
                /// The vector shell itself is destroyed here - trivial after the parallel clear.
            }
            return watch.elapsedSeconds();
        });
    };

    for (size_t fanout : fanouts)
    {
        const size_t bits = static_cast<size_t>(std::countr_zero(fanout));
        double seconds = measure_once({bits});
        double bw = static_cast<double>(rows) * static_cast<double>(row_width) / seconds;
        result.points.push_back({fanout, bw});
        fmt::print("{:>10}{:>14.2f}\n", fanout, bw / 1e9);
    }

    /// One true 2-pass point: 16384 partitions via the same 7+7 bit split
    /// computePassBits(16384, 8192) produces. Used directly by predict() for n_pass == 2
    /// predictions instead of extrapolating from single-pass numbers, since it already
    /// contains the refine path's single-threaded-per-group behavior and the frees between
    /// passes.
    if (cfg.max_partitions > MAX_FANOUT_PER_PASS)
    {
        double seconds = measure_once({7, 7});
        double bw = static_cast<double>(rows) * static_cast<double>(row_width) / seconds;
        result.two_pass_eff_bytes_per_sec = bw;
        fmt::print("{:>10}{:>14.2f}\n", "16384 (2-pass)", bw / 1e9);
    }

    return result;
}


/// ---------------------------------------------------------------------------------------------
/// Hash table size model, shared by the sweeps and the analytical model.
/// ---------------------------------------------------------------------------------------------
/// Exact size of HashMap<UInt64, UInt64> holding `distinct` keys: 16-byte cells, load factor 0.5,
/// initial degree 8, growth by two degrees up to degree 23, then by one.
size_t htBytesForDistinct(size_t distinct)
{
    size_t degree = 8;
    while (distinct > (1ULL << (degree - 1)))
        degree += (degree >= 23 ? 1 : 2);
    return (1ULL << degree) * 16;
}

/// Exact size of a HashMap constructed with a size hint of `n` (the reserve path the radix
/// benches now use): grower.set gives degree = max(8, floor(log2(n - 1)) + 2), i.e. 2n..4n
/// cells - tighter than the 2n..8n of the insertion-growth ladder above.
size_t htBytesForDistinctReserved(size_t n)
{
    size_t degree = 8;
    if (n > 1)
        degree = std::max<size_t>(8, static_cast<size_t>(std::log2(static_cast<double>(n - 1))) + 2);
    return (1ULL << degree) * 16;
}

std::vector<size_t> tableSweepDistincts(const Config & cfg, size_t l2)
{
    /// predict() consults the RP curves only at htBytesForDistinct(D / P*):
    ///   - uncapped, the P* loop guarantees reserved per-partition bytes <= L2, so the
    ///     consulted (growth-ladder) label is < 4 * L2;
    ///   - capped at max_partitions, the largest grid point D = 2^30 gives
    ///     htBytesForDistinct(2^30) / bit_ceil(max_partitions).
    /// x2 for one interpolation bracket past the largest consulted point. Larger per-thread
    /// tables would only feed the informational "spilling" printout at ~T x 256 MiB of RSS.
    const size_t cap = std::min(cfg.max_table_bytes,
        2 * std::max(4 * l2, htBytesForDistinct(1ULL << 30) / std::bit_ceil(cfg.max_partitions)));

    std::vector<size_t> result;
    for (size_t d = 256; htBytesForDistinct(d) <= cap; d *= 4)
        result.push_back(d);
    return result;
}

/// Empty header Block for one side: prefix + "key" column plus payload columns, all UInt64.
Block makeHeader(const std::string & prefix, size_t payload_columns)
{
    Block header;
    auto type = std::make_shared<DataTypeUInt64>();
    header.insert(ColumnWithTypeAndName(ColumnUInt64::create(), type, prefix + "key"));
    for (size_t c = 0; c < payload_columns; ++c)
        header.insert(ColumnWithTypeAndName(ColumnUInt64::create(), type, fmt::format("{}p{}", prefix, c)));
    return header;
}

/// Per-thread build inputs for the radix per-partition kernels: for every thread, blocks holding
/// exactly `distinct` rows covering the thread's disjoint key domain
/// (tid * KEY_DOMAIN_STRIDE + [0, distinct)) once each, in shuffled order. Unlike striping one
/// shared block list over threads (which at block granularity leaves most threads without data
/// for small `distinct`), this guarantees every thread an identical, duplicate-free build side.
std::vector<std::vector<Block>> generatePerThreadBuildBlocks(WorkerPool & pool, size_t distinct, size_t payload_columns)
{
    const size_t threads = pool.size();
    std::vector<std::vector<Block>> result(threads);
    auto type = std::make_shared<DataTypeUInt64>();

    pool.run([&](size_t tid)
    {
        const UInt64 offset = tid * KEY_DOMAIN_STRIDE;
        auto & blocks = result[tid];
        for (size_t begin = 0; begin < distinct; begin += DEFAULT_BLOCK_SIZE)
        {
            const size_t n = std::min<size_t>(DEFAULT_BLOCK_SIZE, distinct - begin);
            Block block;

            auto key_col = ColumnUInt64::create(n);
            auto & key_data = key_col->getData();
            for (size_t i = 0; i < n; ++i)
                key_data[i] = offset + shuffledIndex(begin + i, distinct);
            block.insert(ColumnWithTypeAndName(std::move(key_col), type, "b_key"));

            for (size_t c = 0; c < payload_columns; ++c)
            {
                auto col = ColumnUInt64::create(n);
                auto & data = col->getData();
                for (size_t i = 0; i < n; ++i)
                    data[i] = begin + i;
                block.insert(ColumnWithTypeAndName(std::move(col), type, fmt::format("b_p{}", c)));
            }

            blocks.push_back(std::move(block));
        }
    });

    return result;
}

/// ---------------------------------------------------------------------------------------------
/// Kernel 3a: radix per-partition build. Each thread builds `reps` private real HashJoins in
/// sequence (the radix join's per-partition builds) from its own duplicate-free build input;
/// join construction, map growth and stored-block saving are inside the timed region.
/// Sweep the number of distinct keys (i.e. table size).
/// ---------------------------------------------------------------------------------------------
Curve runBuildKernelRP(const Config & cfg, WorkerPool & pool, size_t l2)
{
    const size_t threads = cfg.threads;
    Curve curve;

    fmt::print("\n=== HT build, radix per-partition (real HashJoin, size sweep) ===\n");
    fmt::print("{:>12}{:>14}{:>6}{:>12}{:>14}\n", "distinct", "table", "reps", "ns/row", "Mrows/s");

    const Block left_header = makeHeader("p_", cfg.probe_payload_columns);
    const Block right_header = makeHeader("b_", cfg.build_payload_columns);
    auto table_join = makeTableJoin(left_header, right_header);
    auto shared_right_header = std::make_shared<const Block>(right_header);

    for (size_t distinct : tableSweepDistincts(cfg, l2))
    {
        auto per_thread_blocks = generatePerThreadBuildBlocks(pool, distinct, cfg.build_payload_columns);

        /// Enough sequential per-partition builds per thread to make the timing meaningful at
        /// small table sizes — mirroring a radix join where each worker builds many partitions.
        const size_t reps = std::max<size_t>(1, cfg.tuples / threads / distinct);
        const size_t actual_rows = distinct * threads * reps;

        double seconds = medianTime(cfg.runs, [&]
        {
            std::vector<std::vector<std::shared_ptr<HashJoin>>> joins(threads);
            double elapsed = pool.run([&](size_t tid)
            {
                auto & my_joins = joins[tid];
                my_joins.resize(reps);
                for (size_t r = 0; r < reps; ++r)
                {
                    /// Mirrors the radix join reserving each partition table from its scatter
                    /// histogram (see RadixHashJoinBench::build): each table inserts exactly
                    /// `distinct` rows, so reserving eliminates all rehash growth.
                    my_joins[r] = std::make_shared<HashJoin>(
                        table_join, shared_right_header, /*any_take_last_row*/ false, /*reserve_num*/ distinct,
                        fmt::format("bench{}_{}", tid, r), /*use_two_level_maps*/ false);
                    for (const auto & block : per_thread_blocks[tid])
                        my_joins[r]->addBlockToJoin(block, /*check_limits*/ false);
                    my_joins[r]->onBuildPhaseFinish();
                }
            });
            /// Untimed: fresh joins per iteration, destroyed in parallel after timing.
            pool.run([&](size_t tid) { joins[tid].clear(); });
            return elapsed;
        });

        const double bytes = static_cast<double>(htBytesForDistinct(distinct));
        const double ns_per_row = seconds * 1e9 / static_cast<double>(actual_rows);
        curve.points.emplace_back(bytes, ns_per_row);

        fmt::print("{:>12}{:>14}{:>6}{:>12.3f}{:>14.1f}\n", distinct, formatBytes(bytes), reps, ns_per_row, 1000.0 / ns_per_row);
    }

    return curve;
}

/// ---------------------------------------------------------------------------------------------
/// Kernel 3b: non-partitioned build. The timed region is the real ConcurrentHashJoin build
/// phase: concurrent addBlockToJoin (with its internal hash/selector dispatch) plus the
/// onBuildPhaseFinish two-level bucket merge. Sweep the number of distinct keys.
/// ---------------------------------------------------------------------------------------------
Curve runBuildKernelNP(const Config & cfg, WorkerPool & pool)
{
    Curve curve;

    fmt::print("\n=== HT build, non-partitioned (real ConcurrentHashJoin, size sweep) ===\n");
    fmt::print("{:>12}{:>14}{:>12}{:>14}\n", "distinct", "table", "ns/row", "Mrows/s");

    const Block left_header = makeHeader("p_", cfg.probe_payload_columns);
    const Block right_header = makeHeader("b_", cfg.build_payload_columns);

    /// A single shared table can be swept further than T private ones.
    for (size_t distinct = 256; htBytesForDistinct(distinct) <= cfg.max_table_bytes * 16; distinct *= 4)
    {
        const size_t rows = std::max(cfg.tuples / 4, distinct);
        auto blocks = generateBlocks(pool, rows, cfg.build_payload_columns, "b_", globalDomainKeys(distinct), cfg.seed + distinct);
        const size_t actual_rows = totalRows(blocks);

        double seconds = medianTime(cfg.runs, [&]
        {
            /// Size-hint statistics keyed by the sweep point: the warmup iteration populates the
            /// cache, so all timed builds preallocate like the steady state of repeated queries.
            ConcurrentHashJoinBench bench(pool, left_header, right_header, /*stats_key*/ intHash64(0xB0 + distinct));
            Stopwatch watch;
            bench.build(blocks);
            return watch.elapsedSeconds();
            /// Untimed: the join is destroyed at scope end after the measurement.
        });

        const double bytes = static_cast<double>(htBytesForDistinct(distinct));
        const double ns_per_row = seconds * 1e9 / static_cast<double>(actual_rows);
        curve.points.emplace_back(bytes, ns_per_row);

        fmt::print("{:>12}{:>14}{:>12.3f}{:>14.1f}\n", distinct, formatBytes(bytes), ns_per_row, 1000.0 / ns_per_row);
    }

    return curve;
}

/// ---------------------------------------------------------------------------------------------
/// Kernel 4a: radix per-partition probe+gather. Per-thread private real HashJoin instances are
/// rebuilt per iteration (untimed); the timed region is joinBlock per probe block plus draining
/// the result into real output Blocks (probe, gather and output materialization fused, with
/// production prefetching), exactly what the radix join runs per partition.
/// ---------------------------------------------------------------------------------------------
Curve runProbeKernelRP(const Config & cfg, WorkerPool & pool, size_t l2)
{
    const size_t threads = cfg.threads;
    Curve curve;

    fmt::print("\n=== HT probe+gather, radix per-partition (real HashJoin, size sweep, hit rate {}) ===\n", cfg.hit_rate);
    fmt::print("{:>12}{:>14}{:>12}{:>14}\n", "distinct", "table", "ns/row", "Mrows/s");

    const Block left_header = makeHeader("p_", cfg.probe_payload_columns);
    const Block right_header = makeHeader("b_", cfg.build_payload_columns);
    auto table_join = makeTableJoin(left_header, right_header);
    auto shared_right_header = std::make_shared<const Block>(right_header);

    for (size_t distinct : tableSweepDistincts(cfg, l2))
    {
        /// Each thread gets its own duplicate-free build side covering its disjoint key domain
        /// exactly once, so every thread has a fully populated private table and the INNER ALL
        /// output is exactly one row per matching probe row, as the model assumes.
        auto build_blocks = generatePerThreadBuildBlocks(pool, distinct, cfg.build_payload_columns);
        auto probe_blocks = generateBlocks(pool, cfg.tuples, cfg.probe_payload_columns, "p_",
                                           probeKeys(distinct, threads, cfg.hit_rate, /*per_thread_domain=*/ true),
                                           cfg.seed + distinct + 1);
        const size_t probe_rows = totalRows(probe_blocks);

        std::vector<std::shared_ptr<HashJoin>> joins(threads);

        auto rebuild = [&]
        {
            pool.run([&](size_t tid)
            {
                /// Mirrors the radix join reserving each partition table from its scatter
                /// histogram (see RadixHashJoinBench::build): each table inserts exactly
                /// `distinct` rows, so reserving eliminates all rehash growth.
                joins[tid] = std::make_shared<HashJoin>(
                    table_join, shared_right_header, /*any_take_last_row*/ false, /*reserve_num*/ distinct,
                    fmt::format("bench{}", tid), /*use_two_level_maps*/ false);
                for (const auto & block : build_blocks[tid])
                    joins[tid]->addBlockToJoin(block, /*check_limits*/ false);
                joins[tid]->onBuildPhaseFinish();
            });
        };

        double seconds = medianTime(cfg.runs, [&]
        {
            rebuild(); /// untimed: fresh joins for every iteration
            return pool.run([&](size_t tid)
            {
                size_t local_rows = 0;
                for (size_t b = tid; b < probe_blocks.size(); b += threads)
                    local_rows += drainJoinResult(joins[tid]->joinBlock(probe_blocks[b]));
                g_sink += local_rows;
                /// output Blocks deallocated here, inside the timed region.
            });
        });

        pool.run([&](size_t tid) { joins[tid].reset(); }); /// free before the next sweep point

        const double bytes = static_cast<double>(htBytesForDistinct(distinct));
        const double ns_per_row = seconds * 1e9 / static_cast<double>(probe_rows);
        curve.points.emplace_back(bytes, ns_per_row);

        fmt::print("{:>12}{:>14}{:>12.3f}{:>14.1f}\n", distinct, formatBytes(bytes), ns_per_row, 1000.0 / ns_per_row);
    }

    return curve;
}

/// ---------------------------------------------------------------------------------------------
/// Kernel 4b: non-partitioned probe+gather. The real ConcurrentHashJoin is rebuilt per
/// iteration (untimed); the timed region is the shared-map joinBlock probe from all T threads
/// with real output Blocks materialized and dropped.
/// ---------------------------------------------------------------------------------------------
Curve runProbeKernelNP(const Config & cfg, WorkerPool & pool)
{
    Curve curve;

    fmt::print("\n=== HT probe+gather, non-partitioned (real ConcurrentHashJoin, size sweep, hit rate {}) ===\n", cfg.hit_rate);
    fmt::print("{:>12}{:>14}{:>12}{:>14}\n", "distinct", "table", "ns/row", "Mrows/s");

    const Block left_header = makeHeader("p_", cfg.probe_payload_columns);
    const Block right_header = makeHeader("b_", cfg.build_payload_columns);

    for (size_t distinct = 256; htBytesForDistinct(distinct) <= cfg.max_table_bytes * 16; distinct *= 4)
    {
        /// Build side is duplicate-free (rows == distinct keys), so the INNER ALL output is
        /// exactly one row per matching probe row, as the model assumes.
        auto build_blocks = generateBlocks(pool, distinct, cfg.build_payload_columns, "b_", globalDomainKeys(distinct), cfg.seed + distinct);
        auto probe_blocks = generateBlocks(pool, cfg.tuples, cfg.probe_payload_columns, "p_",
                                           probeKeys(distinct, cfg.threads, cfg.hit_rate, /*per_thread_domain=*/ false),
                                           cfg.seed + distinct + 1);
        const size_t probe_rows = totalRows(probe_blocks);

        double seconds = medianTime(cfg.runs, [&]
        {
            /// Untimed: fresh join built for every iteration.
            ConcurrentHashJoinBench bench(pool, left_header, right_header, /*stats_key*/ intHash64(0xF0 + distinct));
            bench.build(build_blocks);

            Stopwatch watch;
            size_t rows = bench.probe(probe_blocks, nullptr);
            double elapsed = watch.elapsedSeconds();
            g_sink += rows;
            return elapsed;
        });

        const double bytes = static_cast<double>(htBytesForDistinct(distinct));
        const double ns_per_row = seconds * 1e9 / static_cast<double>(probe_rows);
        curve.points.emplace_back(bytes, ns_per_row);

        fmt::print("{:>12}{:>14}{:>12.3f}{:>14.1f}\n", distinct, formatBytes(bytes), ns_per_row, 1000.0 / ns_per_row);
    }

    return curve;
}


/// ---------------------------------------------------------------------------------------------
/// Kernel 5: gather. From per-block match lists, materialize an output Block: build-side
/// columns filled per matched RowRef via a devirtualized per-row copy, mirroring production's
/// devirtualized route (`IColumn::fillFromBlocksAndRowNumbers` / the internal
/// `fillColumnFromBlocksAndRowNumbers`, `IColumn.cpp:704-732`); probe-side columns expanded via
/// `IColumn::replicate`. The Block is created, filled and dropped inside the timed region.
/// Sweep the stored-build-side working set. Production's other route,
/// `IColumn::fillFromRowRefs`, coalesces runs of consecutive rows from the same RowRefList and
/// can beat the devirtualized per-row route for duplicate-heavy keys; this kernel does not
/// model that, so its printed ns/match is an upper bound (i.e. pessimistic) in that case.
/// ---------------------------------------------------------------------------------------------
Curve runGatherKernel(const Config & cfg, WorkerPool & pool, const std::vector<Block> & probe_blocks)
{
    const size_t threads = cfg.threads;
    Curve curve;

    const size_t stored_block_count = std::max<size_t>(1, cfg.gather_bytes / cfg.buildRowWidth() / DEFAULT_BLOCK_SIZE);
    const size_t stored_rows = stored_block_count * DEFAULT_BLOCK_SIZE;
    auto stored_blocks = generateBlocks(pool, stored_rows, cfg.build_payload_columns, "b_", uniqueKeys(stored_rows), cfg.seed + 12345);
    const size_t stored_block_bytes = DEFAULT_BLOCK_SIZE * cfg.buildRowWidth();
    const size_t build_columns = stored_blocks.front().columns();

    fmt::print("\n=== gather (stored build side sweep, output = CH Block, hit rate {}) ===\n", cfg.hit_rate);
    fmt::print("{:>16}{:>14}{:>16}\n", "working set", "ns/match", "Mmatches/s");

    struct BlockMatches
    {
        PaddedPODArray<UInt64> refs;
        IColumn::Offsets offsets;
    };

    const UInt64 hit_threshold = cfg.hit_rate >= 1.0
        ? std::numeric_limits<UInt64>::max()
        : static_cast<UInt64>(cfg.hit_rate * static_cast<double>(std::numeric_limits<UInt64>::max()));

    std::vector<size_t> sweep_blocks;
    for (size_t k = 1; k < stored_block_count; k *= 4)
        sweep_blocks.push_back(k);
    sweep_blocks.push_back(stored_block_count);

    for (size_t k : sweep_blocks)
    {
        /// Untimed prep: match lists (they are an input to the gather phase).
        std::vector<BlockMatches> matches(probe_blocks.size());
        std::atomic<size_t> total_matches{0};
        pool.run([&](size_t tid)
        {
            pcg64_fast rng(cfg.seed * 31 + tid + k);
            size_t local_matches = 0;
            for (size_t b = tid; b < probe_blocks.size(); b += threads)
            {
                const size_t n = probe_blocks[b].rows();
                auto & m = matches[b];
                m.offsets.resize(n);
                for (size_t i = 0; i < n; ++i)
                {
                    if (rng() <= hit_threshold)
                        m.refs.push_back(packRowRef(rng() % k, rng() % DEFAULT_BLOCK_SIZE));
                    m.offsets[i] = m.refs.size();
                }
                local_matches += m.refs.size();
            }
            total_matches += local_matches;
        });

        /// Untimed prep: per-column raw source pointers for the k stored blocks, precomputed so
        /// the timed region below can copy per row without virtual dispatch (see the kernel's
        /// header comment).
        std::vector<std::vector<const UInt64 *>> src(build_columns, std::vector<const UInt64 *>(k));
        for (size_t j = 0; j < build_columns; ++j)
            for (size_t b = 0; b < k; ++b)
                src[j][b] = assert_cast<const ColumnUInt64 &>(*stored_blocks[b].getByPosition(j).column).getData().data();

        double seconds = medianTime(cfg.runs, [&]
        {
            return pool.run([&](size_t tid)
            {
                for (size_t b = tid; b < probe_blocks.size(); b += threads)
                {
                    const auto & m = matches[b];

                    Block out;
                    for (size_t j = 0; j < build_columns; ++j)
                    {
                        auto dst = ColumnUInt64::create();
                        auto & data = dst->getData();
                        data.reserve(m.refs.size());
                        for (UInt64 ref : m.refs)
                            data.push_back(src[j][refBlock(ref)][refRow(ref)]);
                        const auto & sample = stored_blocks.front().getByPosition(j);
                        out.insert(ColumnWithTypeAndName(std::move(dst), sample.type, sample.name));
                    }
                    for (size_t j = 0; j < probe_blocks[b].columns(); ++j)
                    {
                        const auto & src_col = probe_blocks[b].getByPosition(j);
                        out.insert(ColumnWithTypeAndName(src_col.column->replicate(m.offsets), src_col.type, src_col.name));
                    }

                    g_sink += out.rows();
                    /// output Block deallocated here, inside the timed region.
                }
            });
        });

        const double working_set = static_cast<double>(k) * static_cast<double>(stored_block_bytes);
        const double ns_per_match = seconds * 1e9 / static_cast<double>(total_matches.load());
        curve.points.emplace_back(working_set, ns_per_match);

        fmt::print("{:>16}{:>14.3f}{:>16.1f}\n", formatBytes(working_set), ns_per_match, 1000.0 / ns_per_match);
    }

    return curve;
}


/// ---------------------------------------------------------------------------------------------
/// Analytical model.
/// ---------------------------------------------------------------------------------------------
struct ModelInputs
{
    double memcpy_bytes_per_sec = 0;
    std::vector<ScatterPoint> scatter;
    double scatter_peak = 0;
    double scatter_two_pass_eff = 0; /// measured effective bytes/s of a full 2-pass scatter (0 = not measured)
    size_t f_max = 2;
    Curve build_np;   /// real ConcurrentHashJoin build phase, ns/row vs table size
    Curve build_rp;   /// real per-thread HashJoin build (per-partition shape), ns/row vs table size
    Curve probe_np;   /// real ConcurrentHashJoin probe+gather, ns/row vs table size
    Curve probe_rp;   /// real per-thread HashJoin probe+gather (per-partition shape), ns/row vs table size
    Curve gather;     /// standalone gather term (informational, not used by predict)
    size_t l2 = 0;
    size_t llc = 0;
    size_t w_b = 16;
    size_t w_p = 16;
    size_t max_partitions = 16384;
    size_t threads = 1;

    double scatterBytesPerSec(size_t fanout) const
    {
        chassert(!scatter.empty());
        if (fanout <= scatter.front().fanout)
            return scatter.front().bytes_per_sec;
        if (fanout >= scatter.back().fanout)
            return scatter.back().bytes_per_sec;
        for (size_t i = 1; i < scatter.size(); ++i)
        {
            if (fanout <= scatter[i].fanout)
            {
                const double x0 = std::log2(static_cast<double>(scatter[i - 1].fanout));
                const double x1 = std::log2(static_cast<double>(scatter[i].fanout));
                const double x = std::log2(static_cast<double>(fanout));
                const double f = (x - x0) / (x1 - x0);
                return scatter[i - 1].bytes_per_sec + f * (scatter[i].bytes_per_sec - scatter[i - 1].bytes_per_sec);
            }
        }
        return scatter.back().bytes_per_sec;
    }
};

struct Prediction
{
    double np_build_sec = 0;
    double np_probe_sec = 0; /// probe+gather (fused, as in the real joinBlock)
    double rp_scatter_sec = 0;
    double rp_build_sec = 0;
    double rp_probe_sec = 0; /// probe+gather (fused)
    size_t p_star = 1;
    size_t n_pass = 0;

    double npTotal() const { return np_build_sec + np_probe_sec; }
    double rpTotal() const { return rp_scatter_sec + rp_build_sec + rp_probe_sec; }
};

Prediction predict(const ModelInputs & m, double n_b, double n_p, double distinct)
{
    Prediction p;

    const size_t d = std::max<size_t>(1, static_cast<size_t>(distinct));
    const double table_bytes = static_cast<double>(htBytesForDistinct(d));

    /// NPHJ: both terms measured with the real ConcurrentHashJoin (build includes its internal
    /// hash/selector dispatch and the bucket merge; probe includes gather and output
    /// materialization, fused as in joinBlock).
    p.np_build_sec = n_b * m.build_np.at(table_bytes) * 1e-9;
    p.np_probe_sec = n_p * m.probe_np.at(table_bytes) * 1e-9;

    /// RPHJ: enough partitions for both cache residency and thread parallelism. The probe of a
    /// partition touches the hash table plus the partition's stored build rows (payload gather
    /// through RowRefs), so the L2 budget applies to their sum (measured best at 1B rows:
    /// HT + build within L2 beat both coarser and finer partitioning).
    const double budget = static_cast<double>(m.l2);
    const auto partition_bytes = [&](size_t part)
    {
        /// Real per-partition working set: the grower snaps per table, so this is
        /// htBytes(D/P), NOT htBytes(D)/P - the two differ by up to 2x either way.
        return static_cast<double>(htBytesForDistinctReserved(std::max<size_t>(1, d / part)))
            + (n_b / static_cast<double>(part)) * static_cast<double>(m.w_b);
    };
    size_t p_star = 1;
    while (partition_bytes(p_star) > budget && p_star < m.max_partitions)
        p_star *= 2;
    if (p_star > 1)
        p_star = std::min(std::max(p_star, std::bit_ceil(m.threads)), std::bit_ceil(m.max_partitions));
    p.p_star = p_star;

    if (p_star == 1)
    {
        /// A radix join with one partition degenerates to the non-partitioned join.
        p.rp_build_sec = p.np_build_sec;
        p.rp_probe_sec = p.np_probe_sec;
        return p;
    }

    /// Pass split mirrors what the radix join actually executes (computePassBits with the
    /// measured, memory-safety-clamped F_max - see the F_max wiring in main()).
    const size_t total_bits = static_cast<size_t>(std::countr_zero(p_star));
    const size_t f_bits = std::max<size_t>(1, static_cast<size_t>(std::bit_width(std::bit_floor(m.f_max)) - 1));
    p.n_pass = (total_bits + f_bits - 1) / f_bits;
    const size_t per_pass_bits = (total_bits + p.n_pass - 1) / p.n_pass;
    const size_t per_pass_fanout = 1ULL << per_pass_bits;

    const double scatter_bytes = n_b * static_cast<double>(m.w_b) + n_p * static_cast<double>(m.w_p);
    if (p.n_pass == 2 && m.scatter_two_pass_eff > 0)
        /// A directly measured 2-pass point already contains both passes, the refine path's
        /// single-threaded-per-group behavior, and the frees between passes - more accurate
        /// than n_pass * single-pass bandwidth. No measured point exists beyond n_pass == 2.
        p.rp_scatter_sec = scatter_bytes / m.scatter_two_pass_eff;
    else
        p.rp_scatter_sec = static_cast<double>(p.n_pass) * scatter_bytes / m.scatterBytesPerSec(per_pass_fanout);

    /// Per-partition terms measured with real per-thread HashJoin instances. Curve lookups use
    /// htBytesForDistinct (the same label function the sweeps record points with) applied to
    /// the per-partition key count; htBytesForDistinctReserved above is used ONLY for the
    /// physical L2-fit test above - the two must not be unified, since the sweep curves are
    /// labeled by htBytesForDistinct even though the underlying tables are reserve()'d to
    /// `distinct` (see the reserve_num comments in runBuildKernelRP/runProbeKernelRP).
    const double per_part_label = static_cast<double>(htBytesForDistinct(std::max<size_t>(1, d / p_star)));
    p.rp_build_sec = n_b * m.build_rp.at(per_part_label) * 1e-9;
    p.rp_probe_sec = n_p * m.probe_rp.at(per_part_label) * 1e-9;

    return p;
}

struct Regime
{
    std::string name;
    std::function<size_t(size_t)> distinct_of_nb;
};

std::vector<Regime> gridRegimes()
{
    return
    {
        {"unique (D = N_b)", [](size_t n_b) { return n_b; }},
        {"dup x8 (D = N_b/8)", [](size_t n_b) { return std::max<size_t>(1, n_b / 8); }},
        {"fixed 64K (D = 65536)", [](size_t) { return size_t(65536); }},
    };
}

void printGridAndCrossover(const ModelInputs & m)
{
    const std::vector<size_t> ratios = {1, 10};
    const double np_max_bytes = m.build_np.points.back().first;
    bool any_flat_extrapolated = false;

    fmt::print("\n=== model grid: predicted NPHJ vs RPHJ ===\n");
    fmt::print("{:>22}{:>9}{:>8}{:>12}{:>8}{:>7}{:>13}{:>12}{:>10}{:>10}\n",
        "regime", "N_p/N_b", "N_b", "HT size", "P*", "passes", "T_NP ms", "T_RP ms", "winner", "speedup");

    for (const auto & regime : gridRegimes())
    {
        for (size_t ratio : ratios)
        {
            for (size_t k = 16; k <= 28; k += 2)
            {
                const size_t n_b = 1ULL << k;
                const size_t n_p = n_b * ratio;
                const size_t distinct = regime.distinct_of_nb(n_b);
                auto p = predict(m, static_cast<double>(n_b), static_cast<double>(n_p), static_cast<double>(distinct));

                /// Flag grid points whose HT size sweep label lies beyond the NP curves'
                /// measured maximum: predict() then flat-extrapolates T_NP from the last
                /// measured point instead of interpolating, which is a lower bound.
                const bool flat = static_cast<double>(htBytesForDistinct(distinct)) > np_max_bytes;
                any_flat_extrapolated |= flat;
                const std::string t_np_cell = fmt::format("{:.2f}{}", p.npTotal() * 1e3, flat ? "*" : "");

                const bool radix_wins = p.rpTotal() < p.npTotal();
                fmt::print("{:>22}{:>9}{:>8}{:>12}{:>8}{:>7}{:>13}{:>12.2f}{:>10}{:>10.2f}\n",
                    regime.name, ratio, fmt::format("2^{}", k),
                    formatBytes(static_cast<double>(htBytesForDistinct(distinct))),
                    p.p_star, p.n_pass, t_np_cell, p.rpTotal() * 1e3,
                    radix_wins ? "radix" : "non-part", p.npTotal() / p.rpTotal());
            }
            fmt::print("\n");
        }
    }

    if (any_flat_extrapolated)
        fmt::print("* NP curves flat-extrapolated beyond the measured sweep maximum ({}); T_NP is a lower bound there.\n",
            formatBytes(np_max_bytes));

    fmt::print("NOTE: dup x8 and fixed-64K rows are computed from curves measured on duplicate-free builds "
               "(RightAny-promoted probes, one output row per hit). A real INNER ALL join with duplicate keys "
               "walks RowRefList chains and amplifies output by the duplication factor - those rows are "
               "optimistic lower bounds, not INNER ALL predictions.\n");

    const auto regimes = gridRegimes();

    fmt::print("=== crossover summary ===\n");
    /// The dup regimes' probes are RightAny-promoted point lookups (see the note above), which
    /// do not model an INNER ALL join walking RowRefList chains over duplicate keys; only the
    /// unique-keys regime's crossover is a meaningful prediction.
    for (size_t ratio : ratios)
    {
        const auto & regime = regimes[0];
        std::optional<size_t> crossover;
        for (size_t k = 14; k <= 30; ++k)
        {
            const size_t n_b = 1ULL << k;
            const size_t distinct = regime.distinct_of_nb(n_b);
            auto p = predict(m, static_cast<double>(n_b), static_cast<double>(n_b * ratio), static_cast<double>(distinct));
            if (p.rpTotal() < p.npTotal() * 0.999)
            {
                crossover = n_b;
                break;
            }
        }

        fmt::print("  regime [{}], N_p/N_b = {}: ", regime.name, ratio);
        if (!crossover)
        {
            fmt::print("radix partitioning never wins for N_b up to 2^30\n");
            continue;
        }

        const size_t n_b = *crossover;
        const size_t distinct = regime.distinct_of_nb(n_b);
        const size_t table_bytes = htBytesForDistinct(distinct);
        auto p = predict(m, static_cast<double>(n_b), static_cast<double>(n_b * ratio), static_cast<double>(distinct));
        fmt::print("radix wins from N_b >= {} (D = {}, HT = {} = {:.1f}x LLC, P* = {}, passes = {})\n",
            n_b, distinct, formatBytes(static_cast<double>(table_bytes)),
            static_cast<double>(table_bytes) / static_cast<double>(m.llc), p.p_star, p.n_pass);
    }
    for (size_t ri = 1; ri < regimes.size(); ++ri)
        fmt::print("  regime [{}]: crossover not evaluated (probe/build curves do not model duplicate-key ALL joins)\n", regimes[ri].name);
}


/// ---------------------------------------------------------------------------------------------
/// Fraction crossover: with the probe side SMALLER than the build side, the partitioned join's
/// build-side work (shuffle + per-partition builds, net of NPHJ's own build cost) is an
/// investment that only per-probe-row savings can amortize. For fixed N_b, P* does not depend
/// on N_p, so gain(f) = T_NP - T_RP at N_p = f * N_b is exactly linear in f, and the minimal
/// winning fraction has the closed form
///
///   f* = -gain(0) / gain'(f)
///      = (c_s*w_b - dBuild) / (dPG - c_s*w_p),   c_s = n_pass / B_scatter (per byte)
///
/// The RP probe curve (runProbeKernelRP) probes tables still cache-warm from their untimed
/// rebuild, so it excludes the compulsory reload of each partition's table + stored build rows
/// that a real radix join pays when probing a partition long after building it (all P* builds
/// have evicted each other by then). That reload,
///
///   C_reload = (P* * htBytesReserved(D/P*) + N_b*w_b) / B_read,
///
/// is independent of N_p and matters exactly in this small-probe regime, so f* is reported both
/// without ("warm") and with ("+reload") it, using the measured memcpy bandwidth as B_read.
/// Real-join validation at fractional N_p decides which is closer to reality.
/// ---------------------------------------------------------------------------------------------
void printFractionCrossover(const ModelInputs & m)
{
    const double np_max_bytes = m.build_np.points.back().first;

    fmt::print("\n=== fraction crossover: minimal f = N_p/N_b where the partitioned join wins (unique keys) ===\n");
    fmt::print("{:>8}{:>12}{:>8}{:>7}{:>12}{:>12}{:>12}{:>12}{:>12}{:>12}{:>14}{:>14}\n",
        "N_b", "HT size", "P*", "passes",
        "dBuild ns", "dPG ns", "scat_b ns", "scat_p ns", "reload ms",
        "f* warm", "f* +reload", "min N_p");

    for (size_t k = 22; k <= 30; ++k)
    {
        const size_t n_b = 1ULL << k;
        const double d = static_cast<double>(n_b);
        auto p0 = predict(m, d, /*n_p*/ 0.0, d);
        auto p1 = predict(m, d, /*n_p*/ d, d);

        const std::string nb_label = fmt::format("2^{}{}", k,
            static_cast<double>(htBytesForDistinct(n_b)) > np_max_bytes ? "*" : "");

        if (p1.p_star <= 1)
        {
            fmt::print("{:>8}{:>12}{:>8}{:>7}  (P* = 1: degenerates to the non-partitioned join)\n",
                nb_label, formatBytes(static_cast<double>(htBytesForDistinct(n_b))), p1.p_star, p1.n_pass);
            continue;
        }

        /// All per-row terms derived from predict() itself, so they include the pass split and
        /// the measured 2-pass scatter point exactly as the grid predictions do.
        const double scat_b_ns = p0.rp_scatter_sec / d * 1e9;                       /// build-side shuffle per build row
        const double scat_p_ns = (p1.rp_scatter_sec - p0.rp_scatter_sec) / d * 1e9; /// probe-side shuffle per probe row
        const double d_build_ns = (p0.np_build_sec - p0.rp_build_sec) / d * 1e9;    /// NP build - RP build per build row
        const double d_pg_ns = (p1.np_probe_sec - p1.rp_probe_sec) / d * 1e9;       /// NP probe - RP probe per probe row

        const double gain0 = p0.npTotal() - p0.rpTotal();
        const double slope = (p1.npTotal() - p1.rpTotal()) - gain0; /// net RP gain per unit f

        const double reload_bytes
            = static_cast<double>(p1.p_star)
                * static_cast<double>(htBytesForDistinctReserved(std::max<size_t>(1, n_b / p1.p_star)))
            + d * static_cast<double>(m.w_b);
        const double reload_sec = reload_bytes / m.memcpy_bytes_per_sec;

        /// gain(f) = gain0 - extra + slope * f; the winning f-range depends on both signs:
        ///   slope > 0: probe rows amortize the build-side investment -> wins for f > f*
        ///   slope < 0: each probe row is a net cost -> wins for f < f_cap (build-side gain only)
        const auto fraction_cell = [&](double extra) -> std::string
        {
            const double g0 = gain0 - extra;
            if (slope > 0)
                return g0 >= 0 ? "always" : fmt::format("f>{:.4f}", -g0 / slope);
            if (slope < 0)
                return g0 <= 0 ? "never" : fmt::format("f<{:.4f}", g0 / -slope);
            return g0 > 0 ? "always" : "never";
        };

        std::string min_np = "-";
        if (slope > 0 && gain0 - reload_sec < 0)
            min_np = fmt::format("{:.3g}", (reload_sec - gain0) / slope * d);

        fmt::print("{:>8}{:>12}{:>8}{:>7}{:>12.3f}{:>12.3f}{:>12.3f}{:>12.3f}{:>12.2f}{:>12}{:>14}{:>14}\n",
            nb_label, formatBytes(static_cast<double>(htBytesForDistinct(n_b))), p1.p_star, p1.n_pass,
            d_build_ns, d_pg_ns, scat_b_ns, scat_p_ns, reload_sec * 1e3,
            fraction_cell(0.0), fraction_cell(reload_sec), min_np);
    }

    fmt::print("  f* warm    = minimal probe fraction from the measured curves as-is (RP partitions cache-warm at probe).\n"
               "  f* +reload = same plus the compulsory per-partition reload C_reload charged to the RP probe.\n"
               "  * = NP curves flat-extrapolated beyond the measured sweep maximum; f* is an upper bound there.\n");
}



/// ---------------------------------------------------------------------------------------------
/// Validation: real end-to-end INNER joins (implementations in concurrent_hash_join_bench.cpp
/// and radix_hash_join_bench.cpp) driven through the IJoinBench interface.
/// ---------------------------------------------------------------------------------------------

/// Run the two real joins at one exact (N_b, N_p) point, without measuring the model kernels.
/// Reports the measured wall time of every phase for each repetition.
void runSingleJoin(const Config & cfg, WorkerPool & pool, const CacheInfo & cache, size_t n_b, size_t n_p)
{
    /// The per-partition L2 budget covers the hash table plus the partition's stored build rows
    /// (payload gather through RowRefs touches both). Same partition_bytes shape as predict();
    /// this path runs no measurements to reuse a ModelInputs, so it is inlined here.
    const double budget = static_cast<double>(cache.l2);
    const auto partition_bytes = [&](size_t part)
    {
        return static_cast<double>(htBytesForDistinctReserved(std::max<size_t>(1, n_b / part)))
            + (static_cast<double>(n_b) / static_cast<double>(part)) * static_cast<double>(cfg.buildRowWidth());
    };
    size_t p_star = 1;
    while (partition_bytes(p_star) > budget && p_star < cfg.max_partitions)
        p_star *= 2;
    if (p_star > 1)
        p_star = std::min(std::max(p_star, std::bit_ceil(cfg.threads)), std::bit_ceil(cfg.max_partitions));
    p_star = std::max<size_t>(2, p_star);
    /// No measured F_max exists on this path (no kernel sweeps have run here); apply the same
    /// memory-safety clamp predict()/runValidation apply to the measured F_max.
    const size_t f_max = std::min(MAX_FANOUT_PER_PASS, std::bit_floor(std::max<size_t>(2, cache.l2 / 128)));

    const double table_bytes = static_cast<double>(htBytesForDistinct(n_b));
    fmt::print("\n=== single join: N_b = {}, N_p = {}, unique keys, hit rate {}, HT = {}, build side = {}, P* = {} ===\n",
        n_b, n_p, cfg.hit_rate, formatBytes(table_bytes), formatBytes(static_cast<double>(n_b * cfg.buildRowWidth())), p_star);

    auto build_blocks = generateBlocks(pool, n_b, cfg.build_payload_columns, "b_", uniqueKeys(n_b), cfg.seed + n_b);
    auto probe_blocks = generateBlocks(pool, n_p, cfg.probe_payload_columns, "p_",
                                       probePermutationKeys(n_b, n_p, cfg.hit_rate), cfg.seed + n_b + 1);

    const Block left_header = probe_blocks.front().cloneEmpty();
    const Block right_header = build_blocks.front().cloneEmpty();

    /// Size-hint statistics keyed by the shape: run 0 builds cold and populates the cache,
    /// later runs preallocate the maps (steady state of repeated queries).
    const UInt64 stats_key = intHash64(n_b * 1000003 + n_p);

    if (!cfg.run_nphj)
        fmt::print("  (--algo rphj: skipping NPHJ)\n");
    if (!cfg.run_rphj)
        fmt::print("  (--algo nphj: skipping RPHJ)\n");

    /// Measure each competitor as a self-contained step so the two can be run in either order
    /// (see the order alternation below).
    auto measure_np = [&]() -> std::optional<JoinStats>
    {
        if (!cfg.run_nphj)
            return std::nullopt;
        ConcurrentHashJoinBench bench(pool, left_header, right_header, stats_key);
        return driveJoin(bench, build_blocks, probe_blocks, cfg.verify);
    };
    auto measure_rp = [&](std::string & detail) -> std::optional<JoinStats>
    {
        if (!cfg.run_rphj)
            return std::nullopt;
        RadixHashJoinBench bench(pool, left_header, right_header, f_max);
        auto stats = driveJoin(bench, build_blocks, probe_blocks, cfg.verify);
        detail = bench.phaseBreakdown();
        return stats;
    };

    for (size_t run = 0; run < cfg.runs; ++run)
    {
        /// Alternate which competitor measures first: a fixed order lets the second competitor
        /// reuse the jemalloc extents the first just freed - a systematic page-fault asymmetry.
        const bool np_first = (run % 2 == 0);
        std::optional<JoinStats> np;
        std::optional<JoinStats> rp;
        std::string rp_detail;
        if (np_first)
        {
            np = measure_np();
            rp = measure_rp(rp_detail);
        }
        else
        {
            rp = measure_rp(rp_detail);
            np = measure_np();
        }
        const char * order_tag = np_first ? "NP->RP" : "RP->NP";

        std::string result_check;
        if (cfg.verify && np && rp)
            result_check = np->fingerprint == rp->fingerprint
                ? fmt::format(", results equal (fingerprint {:x})", np->fingerprint)
                : fmt::format(", RESULTS DIFFER (fingerprints {:x} vs {:x})", np->fingerprint, rp->fingerprint);

        /// ProfileEvents are summed over all threads; divide by thread count to get a per-thread
        /// average directly comparable to the wall-clock phase times (build_sec/probe_sec).
        const double inv_threads = 1.0 / static_cast<double>(cfg.threads);

        if (np)
            fmt::print("  run {}: NPHJ total {:.2f} ms (build {:.2f} ms, probe+gather {:.2f} ms, teardown {:.2f} ms; "
                       "match/thr {:.2f} ms, gather/thr {:.2f} ms, dispatch/thr {:.2f} ms); matches {}, order {}\n",
                run, np->total() * 1e3, np->build_sec * 1e3, np->probe_sec * 1e3, np->teardown_sec * 1e3,
                np->probe_profile.match_sec * 1e3 * inv_threads, np->probe_profile.gather_sec * 1e3 * inv_threads,
                np->probe_profile.dispatch_sec * 1e3 * inv_threads, np->matches, order_tag);

        if (rp)
            fmt::print("  run {}: RPHJ total {:.2f} ms (build {:.2f} ms, probe {:.2f} ms, teardown {:.2f} ms; {}; "
                       "match/thr {:.2f} ms, gather/thr {:.2f} ms); matches {}, order {}\n",
                run, rp->total() * 1e3, rp->build_sec * 1e3, rp->probe_sec * 1e3, rp->teardown_sec * 1e3, rp_detail,
                rp->probe_profile.match_sec * 1e3 * inv_threads, rp->probe_profile.gather_sec * 1e3 * inv_threads,
                rp->matches, order_tag);

        if (np && rp)
            fmt::print("  run {}: NPHJ vs RPHJ matches {} vs {}{}{}\n",
                run, np->matches, rp->matches, np->matches == rp->matches ? "" : " MISMATCH", result_check);
    }
}

/// BEP probe-budget sweep: fixed (N_b, N_p), probe consumed in ceil(N_p*w_p / M) waves - each
/// wave is one probe-buffer budget M of scattered probe bytes, probed to completion before the
/// next (RadixHashJoinBench::probeWaves, fused streaming loop). The budget is expressed as a
/// fraction of the build side's total accumulated bytes (stored build rows + reserved hash
/// tables), swept 5%..25% in 5% steps with a 512 MiB floor; a PHJ row (one wave = full probe
/// materialized) and the NPHJ probe of the same N_p bound the sweep from both sides. Growing
/// the budget grows the rows-per-partition-per-visit, i.e. how well each visit amortizes the
/// partition working-set reload.
void runBepWaveSweep(const Config & cfg, WorkerPool & pool, const CacheInfo & cache, size_t n_b, size_t n_p, size_t extra_budget)
{
    /// P*/F_max selection: same as runSingleJoin.
    const double budget = static_cast<double>(cache.l2);
    const auto partition_bytes = [&](size_t part)
    {
        return static_cast<double>(htBytesForDistinctReserved(std::max<size_t>(1, n_b / part)))
            + (static_cast<double>(n_b) / static_cast<double>(part)) * static_cast<double>(cfg.buildRowWidth());
    };
    size_t p_star = 1;
    while (partition_bytes(p_star) > budget && p_star < cfg.max_partitions)
        p_star *= 2;
    if (p_star > 1)
        p_star = std::min(std::max(p_star, std::bit_ceil(cfg.threads)), std::bit_ceil(cfg.max_partitions));
    p_star = std::max<size_t>(2, p_star);
    const size_t f_max = std::min(MAX_FANOUT_PER_PASS, std::bit_floor(std::max<size_t>(2, cache.l2 / 128)));

    fmt::print("\n=== BEP wave sweep: N_b = {}, N_p = {}, unique keys, hit rate {}, HT = {}, build side = {}, P* = {} ===\n",
        n_b, n_p, cfg.hit_rate, formatBytes(static_cast<double>(htBytesForDistinct(n_b))),
        formatBytes(static_cast<double>(n_b * cfg.buildRowWidth())), p_star);

    auto build_blocks = generateBlocks(pool, n_b, cfg.build_payload_columns, "b_", uniqueKeys(n_b), cfg.seed + n_b);
    auto probe_blocks = generateBlocks(pool, n_p, cfg.probe_payload_columns, "p_",
                                       probePermutationKeys(n_b, n_p, cfg.hit_rate), cfg.seed + n_b + 1);
    const Block left_header = probe_blocks.front().cloneEmpty();
    const Block right_header = build_blocks.front().cloneEmpty();

    /// NPHJ probe reference (built once; probe repeated, median).
    double np_probe_sec = 0;
    size_t np_matches = 0;
    {
        ConcurrentHashJoinBench np(pool, left_header, right_header, intHash64(n_b * 1000003 + n_p));
        Stopwatch build_watch;
        np.build(build_blocks);
        const double np_build_sec = build_watch.elapsedSeconds();
        np_probe_sec = medianTime(cfg.runs, [&]
        {
            Stopwatch watch;
            np_matches = np.probe(probe_blocks, nullptr);
            return watch.elapsedSeconds();
        });
        np.teardown();
        fmt::print("  NPHJ reference: build {:.2f} ms, probe+gather {:.2f} ms ({:.3f} ns/row)\n",
            np_build_sec * 1e3, np_probe_sec * 1e3, np_probe_sec * 1e9 / static_cast<double>(n_p));
    }

    RadixHashJoinBench rp(pool, left_header, right_header, f_max);
    Stopwatch build_watch;
    rp.build(build_blocks);
    const double rp_build_sec = build_watch.elapsedSeconds();

    /// The budget's reference quantity: everything the build phase has accumulated by probe
    /// time - the stored (scattered) build rows plus the reserved per-partition hash tables.
    const size_t build_accumulated_bytes = n_b * cfg.buildRowWidth()
        + p_star * htBytesForDistinctReserved(std::max<size_t>(1, n_b / p_star));
    const size_t probe_bytes = n_p * cfg.probeRowWidth();
    constexpr size_t min_budget = 512ULL << 20;

    fmt::print("  RPHJ build: {:.2f} ms ({}); build accumulated bytes (stored rows + reserved HTs): {}\n\n",
        rp_build_sec * 1e3, rp.phaseBreakdown(), formatBytes(static_cast<double>(build_accumulated_bytes)));

    fmt::print("{:>10}{:>14}{:>7}{:>16}{:>12}{:>12}{:>12}{:>12}{:>10}{:>10}\n",
        "budget", "bytes", "waves", "rows/part/wave", "scatter ms", "probe ms", "total ms", "ns/row", "vs NP", "matches");

    /// (label, budget bytes); budget 0 = unbounded (PHJ, one wave).
    std::vector<std::pair<std::string, size_t>> budgets;
    budgets.emplace_back("PHJ", 0);
    for (size_t percent = 5; percent <= 25; percent += 5)
        budgets.emplace_back(fmt::format("{}%", percent),
            std::max(min_budget, build_accumulated_bytes * percent / 100));
    if (extra_budget)
        budgets.emplace_back("extra", extra_budget);

    for (const auto & [label, budget_bytes] : budgets)
    {
        const size_t waves = budget_bytes ? std::max<size_t>(1, (probe_bytes + budget_bytes - 1) / budget_bytes) : 1;

        /// Median by total probe time over cfg.runs (plus one discarded warmup).
        struct Sample { double scatter, join; };
        std::vector<Sample> samples;
        size_t matches = 0;
        rp.probeWaves(probe_blocks, waves, nullptr); /// warmup
        for (size_t r = 0; r < cfg.runs; ++r)
        {
            matches = rp.probeWaves(probe_blocks, waves, nullptr);
            samples.push_back({rp.probeScatterSec(), rp.probeJoinSec()});
        }
        std::sort(samples.begin(), samples.end(),
            [](const Sample & a, const Sample & b) { return a.scatter + a.join < b.scatter + b.join; });
        const Sample & med = samples[samples.size() / 2];
        const double total = med.scatter + med.join;

        fmt::print("{:>10}{:>14}{:>7}{:>16}{:>12.2f}{:>12.2f}{:>12.2f}{:>12.3f}{:>10.2f}{:>10}\n",
            label, formatBytes(static_cast<double>(budget_bytes ? budget_bytes : probe_bytes)), waves,
            n_p / (waves * p_star),
            med.scatter * 1e3, med.join * 1e3, total * 1e3,
            total * 1e9 / static_cast<double>(n_p), np_probe_sec / total,
            matches == np_matches ? "ok" : "MISMATCH");
    }
    rp.teardown();

    fmt::print("\n  budget = max(512 MiB, %% of build accumulated bytes); waves = ceil(probe bytes / budget);\n"
               "  PHJ = unbounded budget (full probe materialized). vs NP = NPHJ probe time / BEP probe total.\n"
               "  All budgets probe the SAME prebuilt partition tables; scatter+probe are per-wave, summed.\n");
}


void runValidation(const Config & cfg, WorkerPool & pool, const ModelInputs & model)
{
    for (size_t ratio : {size_t(1), size_t(10)})
    {
        /// Pick points around the predicted crossover in the unique-keys regime.
        std::optional<size_t> crossover;
        for (size_t k = 14; k <= 30; ++k)
        {
            const size_t n_b = 1ULL << k;
            auto p = predict(model, static_cast<double>(n_b), static_cast<double>(n_b * ratio), static_cast<double>(n_b));
            if (p.rpTotal() < p.npTotal() * 0.999)
            {
                crossover = n_b;
                break;
            }
        }

        std::vector<size_t> points;
        const size_t base = std::max(crossover.value_or(1ULL << 24), size_t(1) << 20);
        for (size_t n_b : {base / 16, base / 4, base, base * 8})
            if (n_b >= (1ULL << 16) && n_b <= cfg.validation_max_rows && n_b * ratio <= 4 * cfg.validation_max_rows
                && (points.empty() || n_b != points.back()))
                points.push_back(n_b);
        if (points.empty())
            points = {1ULL << 22, 1ULL << 24};

        fmt::print("\n=== validation: real joins vs model (unique keys, N_p = {} * N_b, hit rate {}) ===\n", ratio, cfg.hit_rate);
        if (crossover)
            fmt::print("  predicted crossover at N_b = {}\n", *crossover);
        else
            fmt::print("  no predicted crossover; validating at default points\n");

        fmt::print("{:>12}{:>8}{:>13}{:>13}{:>13}{:>13}{:>12}{:>12}{:>10}\n",
            "N_b", "P*", "NP pred ms", "NP meas ms", "RP pred ms", "RP meas ms", "pred win", "meas win", "matches");

        size_t point_idx = 0;
        for (size_t n_b : points)
        {
            const size_t n_p = n_b * ratio;
            auto pred = predict(model, static_cast<double>(n_b), static_cast<double>(n_p), static_cast<double>(n_b));
            const size_t p_star = std::max<size_t>(2, pred.p_star);

            auto build_blocks = generateBlocks(pool, n_b, cfg.build_payload_columns, "b_", uniqueKeys(n_b), cfg.seed + n_b);
            auto probe_blocks = generateBlocks(pool, n_p, cfg.probe_payload_columns, "p_",
                                               probePermutationKeys(n_b, n_p, cfg.hit_rate), cfg.seed + n_b + 1);

            const Block left_header = probe_blocks.front().cloneEmpty();
            const Block right_header = build_blocks.front().cloneEmpty();
            const UInt64 stats_key = intHash64(n_b * 1000003 + n_p);

            auto measure_np = [&]() -> std::optional<JoinStats>
            {
                if (!cfg.run_nphj)
                    return std::nullopt;
                /// Discarded warmup: populates the size-hint statistics at destruction so the
                /// timed build below preallocates, matching the steady state the t_build_np
                /// curve measures instead of a cold first build. Extra untimed cost.
                {
                    ConcurrentHashJoinBench warmup(pool, left_header, right_header, stats_key);
                    warmup.build(build_blocks);
                    warmup.teardown();
                }
                ConcurrentHashJoinBench bench(pool, left_header, right_header, stats_key);
                return driveJoin(bench, build_blocks, probe_blocks, cfg.verify);
            };
            auto measure_rp = [&](std::string & detail) -> std::optional<JoinStats>
            {
                if (!cfg.run_rphj)
                    return std::nullopt;
                RadixHashJoinBench bench(pool, left_header, right_header, model.f_max);
                auto stats = driveJoin(bench, build_blocks, probe_blocks, cfg.verify);
                detail = bench.phaseBreakdown();
                return stats;
            };

            /// Alternate which competitor measures first by point index: a fixed order lets the
            /// second competitor reuse the jemalloc extents the first just freed - a systematic
            /// page-fault asymmetry.
            const bool np_first = (point_idx % 2 == 0);
            ++point_idx;
            std::optional<JoinStats> np;
            std::optional<JoinStats> rp;
            std::string rp_detail;
            if (np_first)
            {
                np = measure_np();
                rp = measure_rp(rp_detail);
            }
            else
            {
                rp = measure_rp(rp_detail);
                np = measure_np();
            }
            const char * order_tag = np_first ? "NP->RP" : "RP->NP";

            const char * pred_win = pred.rpTotal() < pred.npTotal() ? "radix" : "non-part";
            const char * meas_win = (np && rp) ? (rp->total() < np->total() ? "radix" : "non-part") : "-";
            const char * match_check = "-";
            if (np && rp)
            {
                const bool counts_ok = np->matches == rp->matches;
                const bool results_ok = !cfg.verify || np->fingerprint == rp->fingerprint;
                match_check = !counts_ok ? "MISMATCH" : (!results_ok ? "DIFFER" : "ok");
            }

            fmt::print("{:>12}{:>8}{:>13.2f}{:>13.2f}{:>13.2f}{:>13.2f}{:>12}{:>12}{:>10}  order {}\n",
                n_b, p_star, pred.npTotal() * 1e3, np ? np->total() * 1e3 : 0.0,
                pred.rpTotal() * 1e3, rp ? rp->total() * 1e3 : 0.0, pred_win, meas_win, match_check, order_tag);

            /// ProfileEvents are summed over all threads; divide by thread count to get a
            /// per-thread average directly comparable to the wall-clock phase times below.
            const double inv_threads = 1.0 / static_cast<double>(cfg.threads);

            if (np)
                fmt::print("      NP meas (build/probe+gather/teardown): {:.2f} / {:.2f} / {:.2f} ms "
                           "(match/thr {:.2f} / gather/thr {:.2f} / dispatch/thr {:.2f} ms), pred: {:.2f} / {:.2f} ms\n",
                    np->build_sec * 1e3, np->probe_sec * 1e3, np->teardown_sec * 1e3,
                    np->probe_profile.match_sec * 1e3 * inv_threads, np->probe_profile.gather_sec * 1e3 * inv_threads,
                    np->probe_profile.dispatch_sec * 1e3 * inv_threads,
                    pred.np_build_sec * 1e3, pred.np_probe_sec * 1e3);
            if (rp)
                fmt::print("      RP meas (build/probe/teardown): {:.2f} / {:.2f} / {:.2f} ms ({}; match/thr {:.2f} / gather/thr {:.2f} ms); "
                           "pred (scatter/build/probe+gather): {:.2f} / {:.2f} / {:.2f} ms\n",
                    rp->build_sec * 1e3, rp->probe_sec * 1e3, rp->teardown_sec * 1e3, rp_detail,
                    rp->probe_profile.match_sec * 1e3 * inv_threads, rp->probe_profile.gather_sec * 1e3 * inv_threads,
                    pred.rp_scatter_sec * 1e3, pred.rp_build_sec * 1e3, pred.rp_probe_sec * 1e3);
        }
    }
}


/// ---------------------------------------------------------------------------------------------
/// Memory budget: estimate the peak RSS of the heaviest kernels before running anything, and
/// fail closed rather than let a too-large config get OOM-killed partway through a multi-minute
/// run.
/// ---------------------------------------------------------------------------------------------
std::optional<size_t> readMemAvailableKb()
{
    std::ifstream in("/proc/meminfo");
    if (!in)
        return std::nullopt;
    std::string line;
    while (std::getline(in, line))
    {
        if (!line.starts_with("MemAvailable:"))
            continue;
        std::istringstream iss(line.substr(std::string("MemAvailable:").size()));
        size_t kb = 0;
        if (iss >> kb)
            return kb;
        return std::nullopt;
    }
    return std::nullopt;
}

size_t estimatePeakBytes(const Config & cfg, size_t l2)
{
    const size_t inputs = cfg.tuples * (cfg.buildRowWidth() + cfg.probeRowWidth());

    /// RP kernels: T private tables sized at the sweep's largest point, plus each thread's
    /// share of stored build rows, plus the full probe side, plus the shared inputs.
    const auto rp_sweep = tableSweepDistincts(cfg, l2);
    const size_t d_rp_top = rp_sweep.empty() ? size_t(256) : rp_sweep.back();
    const size_t rp_bytes = cfg.threads * (htBytesForDistinct(d_rp_top) + d_rp_top * cfg.buildRowWidth())
        + cfg.tuples * cfg.probeRowWidth() + inputs;

    /// NP kernels: two shared tables sized at the largest point the (unmodified) NP sweep bound
    /// reaches (htBytesForDistinct(d) <= max_table_bytes * 16, same condition as
    /// runBuildKernelNP/runProbeKernelNP), plus the larger of a quarter of the tuple budget or
    /// the table's own row count of build rows, plus the full probe side, plus the shared inputs.
    size_t d_np_top = 256;
    for (size_t d = 256; htBytesForDistinct(d) <= cfg.max_table_bytes * 16; d *= 4)
        d_np_top = d;
    const size_t np_bytes = 2 * htBytesForDistinct(d_np_top)
        + std::max(cfg.tuples / 4, d_np_top) * cfg.buildRowWidth() + cfg.tuples * cfg.probeRowWidth() + inputs;

    /// Gather kernel: the stored build side plus refs/offsets scratch, plus the shared inputs.
    const size_t gather_kernel_bytes = cfg.gather_bytes + 2 * cfg.tuples * 16 + inputs;

    /// Validation: both competitors' build/probe blocks at the largest validation row count,
    /// plus two hash tables.
    const size_t validation_bytes = 2 * cfg.validation_max_rows * cfg.buildRowWidth()
        + 8 * cfg.validation_max_rows * cfg.probeRowWidth() + 2 * htBytesForDistinct(cfg.validation_max_rows);

    const size_t peak = std::max({rp_bytes, np_bytes, gather_kernel_bytes, validation_bytes});
    return static_cast<size_t>(1.2 * static_cast<double>(peak));
}

}


int main(int argc, char ** argv)
{
    namespace po = boost::program_options;

    Config cfg;

    po::options_description desc("hash_join_bandwidth_model options");
    desc.add_options()
        ("help", "produce help message")
        ("threads", po::value<size_t>(), "number of worker threads (default: number of CPU cores)")
        ("build-payload-columns", po::value<size_t>(&cfg.build_payload_columns)->default_value(1), "8-byte payload columns on the build side")
        ("probe-payload-columns", po::value<size_t>(&cfg.probe_payload_columns)->default_value(1), "8-byte payload columns on the probe side")
        ("tuples", po::value<size_t>(&cfg.tuples)->default_value(1ULL << 27), "rows of work per kernel iteration (across all threads)")
        ("hit-rate", po::value<double>(&cfg.hit_rate)->default_value(1.0), "probe hit rate in [0, 1]")
        ("max-partitions", po::value<size_t>(&cfg.max_partitions)->default_value(16384), "maximum partition fanout")
        ("max-table-bytes", po::value<size_t>(&cfg.max_table_bytes)->default_value(256ULL << 20), "maximum per-thread hash table size in the sweep")
        ("gather-bytes", po::value<size_t>(&cfg.gather_bytes)->default_value(4ULL << 30), "maximum stored-build-side working set in the gather sweep")
        ("validation-max-rows", po::value<size_t>(&cfg.validation_max_rows)->default_value(1ULL << 26), "maximum N_b for validation joins")
        ("runs", po::value<size_t>(&cfg.runs)->default_value(3), "timed runs per point (median is reported)")
        ("l1", po::value<size_t>(), "override detected L1d size in bytes")
        ("l2", po::value<size_t>(), "override detected L2 size in bytes")
        ("llc", po::value<size_t>(), "override detected total LLC size in bytes")
        ("seed", po::value<UInt64>(&cfg.seed), "random seed")
        ("quick", po::bool_switch(&cfg.quick), "skip the validation joins")
        ("verify", po::bool_switch(&cfg.verify), "compare NPHJ and RPHJ outputs via order-independent row fingerprints (adds overhead to probe timings)")
        ("join-nb", po::value<size_t>()->default_value(0), "run only the real joins at this exact build-side row count (skips all kernels)")
        ("join-np", po::value<size_t>()->default_value(0), "probe-side row count for --join-nb (default: same as --join-nb)")
        ("bep-nb", po::value<size_t>()->default_value(0), "run the BEP probe-budget wave sweep at this build-side row count (skips all kernels)")
        ("bep-np", po::value<size_t>()->default_value(0), "probe-side row count for --bep-nb (default: same as --bep-nb)")
        ("bep-budget", po::value<size_t>()->default_value(0), "additionally measure this explicit probe-buffer budget in bytes in the BEP sweep")
        ("algo", po::value<std::string>()->default_value("both"), "which real join(s) to build+drive in --join-nb and the validation joins: both (default), nphj, rphj");

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);
    po::notify(options);

    if (options.contains("help"))
    {
        fmt::print("{}\n", fmt::streamed(desc));
        return 0;
    }

    cfg.threads = options.contains("threads") ? options["threads"].as<size_t>() : getNumberOfCPUCoresToUse();
    cfg.hit_rate = std::clamp(cfg.hit_rate, 0.01, 1.0);

    const std::string algo = options["algo"].as<std::string>();
    if (algo == "both")
    {
        cfg.run_nphj = true;
        cfg.run_rphj = true;
    }
    else if (algo == "nphj")
    {
        cfg.run_nphj = true;
        cfg.run_rphj = false;
    }
    else if (algo == "rphj")
    {
        cfg.run_nphj = false;
        cfg.run_rphj = true;
    }
    else
    {
        fmt::print(stderr, "invalid --algo '{}': expected one of both, nphj, rphj\n", algo);
        return 1;
    }

    CacheInfo cache = detectCaches();
    if (options.contains("l1"))
        cache.l1d = options["l1"].as<size_t>();
    if (options.contains("l2"))
        cache.l2 = options["l2"].as<size_t>();
    if (options.contains("llc"))
        cache.llc = options["llc"].as<size_t>();

    fmt::print("=== machine ===\n");
    fmt::print("  threads: {} (ClickHouse thread pool)\n", cfg.threads);
    fmt::print("  L1d: {}, L2: {}, LLC total: {}{}\n",
        formatBytes(static_cast<double>(cache.l1d)), formatBytes(static_cast<double>(cache.l2)),
        formatBytes(static_cast<double>(cache.llc)), cache.detected ? "" : " (detection failed, using defaults)");
    fmt::print("  build row width: {} B, probe row width: {} B\n", cfg.buildRowWidth(), cfg.probeRowWidth());
    fmt::print("  work rows per kernel iteration: {}\n", cfg.tuples);

#if USE_JEMALLOC
    {
        const char * jemalloc_version = nullptr;
        size_t version_size = sizeof(jemalloc_version);
        je_mallctl("version", &jemalloc_version, &version_size, nullptr, 0);
        fmt::print("  allocator: jemalloc {}\n", jemalloc_version ? jemalloc_version : "(unknown version)");
    }
#else
    fmt::print("  allocator: system malloc (jemalloc disabled in this build)\n");
#endif

    {
        const size_t estimated_peak = estimatePeakBytes(cfg, cache.l2);
        fmt::print("  estimated peak memory (heaviest kernel): {}\n", formatBytes(static_cast<double>(estimated_peak)));

        const auto mem_available_kb = readMemAvailableKb();
        if (!mem_available_kb)
        {
            fmt::print("  WARNING: /proc/meminfo unreadable; skipping the peak-memory guard (detection failure, not a guard failure)\n");
        }
        else
        {
            const auto mem_available_bytes = static_cast<double>(*mem_available_kb) * 1024.0;
            fmt::print("  MemAvailable: {}\n", formatBytes(mem_available_bytes));
            if (static_cast<double>(estimated_peak) > 0.8 * mem_available_bytes)
            {
                fmt::print(stderr,
                    "ERROR: estimated peak memory {} exceeds 80% of MemAvailable ({}). Refusing to run "
                    "(fail-close): reduce the workload via --tuples, --max-table-bytes, or --threads.\n",
                    formatBytes(static_cast<double>(estimated_peak)), formatBytes(mem_available_bytes));
                return 1;
            }
        }
    }

    WorkerPool pool(cfg.threads);

    /// Shared immutable input blocks (the only memory reused across iterations).
    if (const size_t join_nb = options["join-nb"].as<size_t>())
    {
        const size_t join_np = options["join-np"].as<size_t>() ? options["join-np"].as<size_t>() : join_nb;
        runSingleJoin(cfg, pool, cache, join_nb, join_np);
        fmt::print("\n(check value: {})\n", g_sink.load());
        return 0;
    }

    if (const size_t bep_nb = options["bep-nb"].as<size_t>())
    {
        const size_t bep_np = options["bep-np"].as<size_t>() ? options["bep-np"].as<size_t>() : bep_nb;
        runBepWaveSweep(cfg, pool, cache, bep_nb, bep_np, options["bep-budget"].as<size_t>());
        fmt::print("\n(check value: {})\n", g_sink.load());
        return 0;
    }

    fmt::print("\ngenerating input blocks...\n");
    auto build_work = generateBlocks(pool, cfg.tuples, cfg.build_payload_columns, "b_", uniqueKeys(cfg.tuples), cfg.seed);
    auto probe_work = generateBlocks(pool, cfg.tuples, cfg.probe_payload_columns, "p_",
                                     probeKeys(cfg.tuples, cfg.threads, cfg.hit_rate, /*per_thread_domain=*/ false), cfg.seed + 1);

    ModelInputs model;
    model.l2 = cache.l2;
    model.llc = cache.llc;
    model.w_b = cfg.buildRowWidth();
    model.w_p = cfg.probeRowWidth();
    model.max_partitions = cfg.max_partitions;
    model.threads = cfg.threads;

    model.memcpy_bytes_per_sec = runMemcpyKernel(cfg, pool, build_work);
    fmt::print("\n=== memcpy baseline ===\n  B_cpy = {:.2f} GB/s (aggregate, block squashing via insertRangeFrom)\n",
        model.memcpy_bytes_per_sec / 1e9);

    {
        auto scatter_measurement = runScatterKernel(cfg, pool, build_work);
        model.scatter = std::move(scatter_measurement.points);
        model.scatter_two_pass_eff = scatter_measurement.two_pass_eff_bytes_per_sec;
    }

    model.scatter_peak = 0;
    for (const auto & sp : model.scatter)
        model.scatter_peak = std::max(model.scatter_peak, sp.bytes_per_sec);

    /// Contiguous 80%-of-peak rule: the largest *prefix* fanout (in increasing order) still at
    /// or above 80% of peak, not a later recovery past a dip - a later recovery would not be a
    /// safe single-pass fanout if intermediate fanouts in between are slower.
    model.f_max = model.scatter.front().fanout;
    for (const auto & sp : model.scatter)
    {
        if (sp.bytes_per_sec < 0.8 * model.scatter_peak)
            break;
        model.f_max = std::max(model.f_max, sp.fanout);
    }
    /// Clamp by memory-safety caps: the compile-time SWWC ceiling, and an L2-derived cap (~76 B
    /// of SWWC state per partition per worker; /128 leaves headroom for the histogram/cursors).
    model.f_max = std::min({model.f_max, MAX_FANOUT_PER_PASS, std::bit_floor(std::max<size_t>(2, cache.l2 / 128))});

    fmt::print("  B_scatter peak = {:.2f} GB/s, F_max (>= 80% of peak, contiguous; drives pass planning) = {}\n",
        model.scatter_peak / 1e9, model.f_max);

    model.build_rp = runBuildKernelRP(cfg, pool, cache.l2);
    model.build_np = runBuildKernelNP(cfg, pool);
    model.probe_rp = runProbeKernelRP(cfg, pool, cache.l2);
    model.probe_np = runProbeKernelNP(cfg, pool);
    model.gather = runGatherKernel(cfg, pool, probe_work);

    const double budget = static_cast<double>(cache.l2);
    fmt::print("\n=== derived model constants (all measured with real ClickHouse join code) ===\n");
    fmt::print("  t_build (radix part): cache-resident {:.3f} ns/row, spilling {:.3f} ns/row\n",
        model.build_rp.at(budget), model.build_rp.points.back().second);
    fmt::print("  t_build (non-part):   cache-resident {:.3f} ns/row, spilling {:.3f} ns/row\n",
        model.build_np.at(budget), model.build_np.points.back().second);
    fmt::print("  t_probe+gather (radix part): cache-resident {:.3f} ns/row, spilling {:.3f} ns/row\n",
        model.probe_rp.at(budget), model.probe_rp.points.back().second);
    fmt::print("  t_probe+gather (non-part):   cache-resident {:.3f} ns/row, spilling {:.3f} ns/row\n",
        model.probe_np.at(budget), model.probe_np.points.back().second);
    fmt::print("  t_gather (standalone, devirtualized per-row copy by RowRef): cache-resident {:.3f} ns/match, spilling {:.3f} ns/match\n",
        model.gather.at(budget), model.gather.points.back().second);
    fmt::print("  per-partition working-set budget C = L2 = {} (reserved table bytes + partition's build rows)\n", formatBytes(budget));

    printGridAndCrossover(model);
    printFractionCrossover(model);

    if (!cfg.quick)
        runValidation(cfg, pool, model);

    /// Prevent the compiler from optimizing the kernels away.
    fmt::print("\n(check value: {})\n", g_sink.load());
    return 0;
}
