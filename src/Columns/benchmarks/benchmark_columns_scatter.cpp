/// Microbenchmark for scatter kernels — Gate-1 instrument of the ColumnsScatter work.
///
/// U0 reference arm: a VERBATIM copy of the pre-migration radix scatter kernels from
/// RadixHashJoin.cpp (namespace `ScatterReference` below). Since U5 the live kernels are in
/// src/Columns/ColumnsScatter.{h,cpp}; the frozen source of truth for this block is the commit
/// named in the opening marker further down. The block is kept byte-identical to that line range:
/// to verify, extract the lines strictly between the markers and diff them against it. It measures
/// the byte-bandwidth of `scatterOne<8>` in both routing modes at representative fanouts covering
/// both regimes (direct below `SWWC_MIN_FANOUT`, SWWC + NT stores at or above it).
///
/// Timed region per iteration = seed(all cursors) + scatterOne<8>(n rows) + drain — exactly the
/// per-(batch, column) work of the in-tree barrier-3 scatter. Histogram, prefix sum and destination
/// allocation are untimed setup. Bytes counted = payload bytes written (n * 8); the UInt16 pids
/// emitted by the key mode are a by-product and are not counted. Destinations are warm steady-state
/// (rewritten every iteration): the reference is a RELATIVE kernel gate, not an end-to-end claim.
///
/// Run (single-threaded, pinned; add `--benchmark_filter=-BM_mt` to skip the thread-sweep cells):
///   taskset -c 8 ./benchmark_columns_scatter --benchmark_repetitions=7 \
///       --benchmark_report_aggregates_only=true --benchmark_format=json
/// The `BM_mt_*` thread-sweep cells must run UNPINNED (pinning serializes the worker pool) and
/// with `->UseRealTime()` semantics already baked in.
/// Fixtures are constructed lazily on each cell's first use, so filtered runs and
/// `--benchmark_list_tests` stay cheap — but an UNFILTERED full-matrix run still accumulates
/// roughly 60 GB of fixture memory by the end (review U4-simplicity-1).

#include <benchmark/benchmark.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsScatter.h>
#include <Common/PODArray.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>

#include <base/defines.h>
#include <base/types.h>

#include <pcg_random.hpp>

#include <atomic>
#include <barrier>
#include <bit>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

namespace DB::ScatterReference
{

/// Prototypes for the verbatim block below. The block is kept byte-identical to the in-tree source;
/// there it lives in an anonymous namespace and needs no prototypes, here the namespace is named
/// (so the copied kernels stay visible to the driver) and `-Wmissing-prototypes` requires these.
size_t scatterBatchRowsTarget(size_t fanout);
bool widthSupportsSwwc(size_t w);
ALWAYS_INLINE UInt32 routeWord(UInt64 key);
ALWAYS_INLINE UInt64 mixStep(UInt64 h, UInt64 x);
ALWAYS_INLINE UInt32 finalizeRoute(UInt64 h);
ALWAYS_INLINE UInt64 foldBytes(UInt64 h, const char * p, size_t w);
ALWAYS_INLINE UInt32 routeWordBytes(const char * p, size_t w);

/// ---- BEGIN verbatim copy of git show 646c2c3b4a2:src/Interpreters/RadixHashJoin/RadixHashJoin.cpp lines 70-345 (pre-U5-migration kernels; this frozen copy IS the U0 bandwidth reference) ----
constexpr size_t LINE_BYTES = 64;
constexpr size_t ELEMS_PER_LINE = LINE_BYTES / sizeof(UInt64);
/// Fanout from which the SWWC + non-temporal path wins over plain per-partition cursors.
constexpr size_t SWWC_MIN_FANOUT = 256;
/// Below this fanout the histogram uses 4 interleaved lanes to break the load-increment-store chain.
constexpr size_t HIST_INTERLEAVE_MAX_FANOUT = 2048;
/// First-pass batch sizing: the boundary cost (cursor sweeps, partial-line flushes) stays a small
/// fraction of the lines written in between.
constexpr size_t SCATTER_BATCH_MIN_ROWS = 256 << 10;
constexpr size_t SCATTER_BATCH_LINES_PER_PARTITION = 64;

/// Partition-plan constants (5.1): the target leaf working set (~L2), the per-pass fanout ceiling
/// (the benchmark's SWWC staging cache ceiling, MAX_FANOUT_PER_PASS), and the per-entry hash-table
/// byte estimate (a cell at 0.5 load factor, matching the bench bandwidth model).
constexpr size_t LEAF_TARGET_BYTES = 1 << 20;
constexpr size_t MAX_FANOUT_PER_PASS = 8192;
constexpr size_t HT_CELL_BYTES = 16;

using NtLine = char __attribute__((vector_size(LINE_BYTES)));

size_t scatterBatchRowsTarget(size_t fanout)
{
    return std::max(SCATTER_BATCH_MIN_ROWS, fanout * SCATTER_BATCH_LINES_PER_PARTITION * ELEMS_PER_LINE);
}

/// SWWC is enabled only for widths that divide the 64-byte line and are covered by the 16-byte
/// minimum alignment of column data (so the per-partition staging line fills to exactly 64 bytes).
bool widthSupportsSwwc(size_t w)
{
    return w == 1 || w == 2 || w == 4 || w == 8 || w == 16;
}

/// Route hashes are deliberately independent of the CRC32C the leaf hash tables use for bucketing:
/// otherwise partition assignment would correlate with in-table bucket placement and each leaf
/// table would see a skewed hash space. The hot single-UInt64 path exactly matches the benchmark:
/// ISO-polynomial CRC32 on aarch64, golden-ratio multiply-shift elsewhere. Wider and composite keys
/// retain the width-generic multiply-shift fold.
ALWAYS_INLINE UInt32 routeWord(UInt64 key)
{
#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32d(-1U, key);
#else
    return static_cast<UInt32>((key * 0x9E3779B97F4A7C15ULL) >> 32);
#endif
}

ALWAYS_INLINE UInt64 mixStep(UInt64 h, UInt64 x)
{
    return (h ^ x) * 0x9E3779B97F4A7C15ULL;
}

ALWAYS_INLINE UInt32 finalizeRoute(UInt64 h)
{
    return static_cast<UInt32>(h >> 32);
}

/// Fold `w` bytes at `p` into the accumulator, 8 bytes at a time with a zero-padded tail.
ALWAYS_INLINE UInt64 foldBytes(UInt64 h, const char * p, size_t w)
{
    size_t i = 0;
    for (; i + 8 <= w; i += 8)
    {
        UInt64 x = 0;
        memcpy(&x, p + i, sizeof(x));
        h = mixStep(h, x);
    }
    if (i < w)
    {
        UInt64 x = 0;
        memcpy(&x, p + i, w - i);
        h = mixStep(h, x);
    }
    return h;
}

/// Compile-time width variant for the hot single-key path (the loop unrolls fully).
template <size_t width>
ALWAYS_INLINE UInt32 routeWordFixed(const char * p)
{
    if constexpr (width == sizeof(UInt64))
    {
        UInt64 key{};
        __builtin_memcpy_inline(&key, p, sizeof(key));
        return routeWord(key);
    }
    else
    {
        return finalizeRoute(foldBytes(0, p, width));
    }
}

ALWAYS_INLINE UInt32 routeWordBytes(const char * p, size_t w)
{
    return finalizeRoute(foldBytes(0, p, w));
}

/// Per-worker scatter state: write cursors (byte-granular), and for the SWWC path one 64-byte
/// staging line per partition plus a byte fill counter. Ported from the benchmark's ScatterScratch,
/// generalized from 8-byte elements to arbitrary fixed widths.
///
/// Invariant: staged bytes for partition p live at staging + p*64 + [m, fill), where
/// m = (uintptr)cursors[p] & 63. seed() seeds `fill` with the cursor misalignment; before the first
/// flush the cursor has not advanced (m == fill start), after the first flush the cursor is
/// line-aligned (m == 0). Column-data bases are >= 16-byte aligned and per-worker start offsets are
/// multiples of the element width, so for the SWWC-enabled widths (1,2,4,8,16) m is a multiple of the
/// width and the staging line fills to exactly 64 bytes.
struct ScatterScratch
{
    size_t fanout = 0;
    bool use_swwc = false;
    PaddedPODArray<char> staging_mem;
    char * staging = nullptr;
    PaddedPODArray<char *> cursors;
    PaddedPODArray<UInt32> fill;

    void init(size_t fanout_, bool use_swwc_)
    {
        fanout = fanout_;
        use_swwc = use_swwc_;
        cursors.resize(fanout);
        if (use_swwc)
        {
            staging_mem.resize(fanout * LINE_BYTES + LINE_BYTES);
            staging = reinterpret_cast<char *>(
                (reinterpret_cast<uintptr_t>(staging_mem.data()) + LINE_BYTES - 1) & ~static_cast<uintptr_t>(LINE_BYTES - 1));
            fill.resize(fanout);
        }
    }

    void setUseSwwc(bool use_swwc_)
    {
        chassert(!use_swwc_ || staging);
        use_swwc = use_swwc_;
    }

    void seed(size_t p, char * cursor)
    {
        cursors[p] = cursor;
        if (use_swwc)
            fill[p] = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cursor) & (LINE_BYTES - 1));
    }

    /// Flush residual staged bytes of every partition and publish the non-temporal stores.
    void drain()
    {
        if (!use_swwc)
            return;
        for (size_t p = 0; p < fanout; ++p)
        {
            const UInt32 f = fill[p];
            if (!f)
                continue;
            char * cur = cursors[p];
            const UInt32 m = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cur) & (LINE_BYTES - 1));
            if (f > m)
            {
                memcpy(cur, staging + p * LINE_BYTES + m, f - m);
                cursors[p] = cur + (f - m);
            }
            fill[p] = 0;
        }
        /// NT stores are weakly ordered; make them visible before the outputs are read.
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
};

/// The routing source per row. The single-column key kernel computes the partition from the key (and
/// optionally emits it as a 2-byte pid); the payload kernels reload the emitted pid.
template <size_t width>
struct RouteFromKey
{
    const char * keys;
    UInt32 shift;
    UInt32 mask;
    UInt16 * pids; /// null when there are no columns to consume the ids

    ALWAYS_INLINE UInt32 partition(size_t i) const
    {
        const UInt32 p = (routeWordFixed<width>(keys + i * width) >> shift) & mask;
        if (pids)
            pids[i] = static_cast<UInt16>(p);
        return p;
    }
};

struct RouteFromKeyGeneric
{
    const char * keys;
    size_t width;
    UInt32 shift;
    UInt32 mask;
    UInt16 * pids;

    ALWAYS_INLINE UInt32 partition(size_t i) const
    {
        const UInt32 p = (routeWordBytes(keys + i * width, width) >> shift) & mask;
        if (pids)
            pids[i] = static_cast<UInt16>(p);
        return p;
    }
};

struct RouteFromPids
{
    const UInt16 * pids;
    ALWAYS_INLINE UInt32 partition(size_t i) const { return pids[i]; }
};

template <size_t width, typename Route>
void scatterDirect(Route route, const char * data, size_t n, char ** cursors)
{
    for (size_t i = 0; i < n; ++i)
    {
        const UInt32 p = route.partition(i);
        char * dst = cursors[p];
        __builtin_memcpy_inline(dst, data + i * width, width);
        cursors[p] = dst + width;
    }
}

template <typename Route>
void scatterDirectGeneric(Route route, const char * data, size_t n, size_t w, char ** cursors)
{
    for (size_t i = 0; i < n; ++i)
    {
        const UInt32 p = route.partition(i);
        char * dst = cursors[p];
        memcpy(dst, data + i * w, w);
        cursors[p] = dst + w;
    }
}

template <size_t width, typename Route>
void scatterSwwc(Route route, const char * data, size_t n, ScatterScratch & scratch)
{
    /// Hoisted like `staging`: the char*/vector NT store defeats TBAA hoisting, so without this the
    /// compiler reloads scratch.cursors/fill.data() every row.
    char * const staging = scratch.staging;
    char ** const cursors = scratch.cursors.data();
    UInt32 * const fill = scratch.fill.data();

    for (size_t i = 0; i < n; ++i)
    {
        const UInt32 p = route.partition(i);
        char * line = staging + static_cast<size_t>(p) * LINE_BYTES;
        UInt32 f = fill[p];
        __builtin_memcpy_inline(line + f, data + i * width, width);
        f += width;
        if (f == LINE_BYTES)
        {
            char * cur = cursors[p];
            const UInt32 m = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cur) & (LINE_BYTES - 1));
            if (m) /// first flush of a misaligned stream: emit the partial head line with regular stores
            {
                __builtin_memcpy(cur, line + m, LINE_BYTES - m);
                cursors[p] = cur + (LINE_BYTES - m);
            }
            else
            {
                __builtin_nontemporal_store(*reinterpret_cast<const NtLine *>(line), reinterpret_cast<NtLine *>(cur));
                cursors[p] = cur + LINE_BYTES;
            }
            f = 0;
        }
        fill[p] = f;
    }
}

template <size_t width, typename Route>
ALWAYS_INLINE void scatterOne(Route route, const char * data, size_t n, bool use_swwc, ScatterScratch & scratch)
{
    if (use_swwc)
        scatterSwwc<width>(route, data, n, scratch);
    else
        scatterDirect<width>(route, data, n, scratch.cursors.data());
}
/// ---- END verbatim copy ----

}

namespace
{

using namespace DB;
using namespace DB::ScatterReference;

/// Pin the copied constants: a drift here means the copy no longer matches the in-tree kernels.
static_assert(LINE_BYTES == 64);
static_assert(ELEMS_PER_LINE == 8);
static_assert(SWWC_MIN_FANOUT == 256);
static_assert(HIST_INTERLEAVE_MAX_FANOUT == 2048);
static_assert(SCATTER_BATCH_MIN_ROWS == 256 << 10);
static_assert(SCATTER_BATCH_LINES_PER_PARTITION == 64);
static_assert(LEAF_TARGET_BYTES == 1 << 20);
static_assert(MAX_FANOUT_PER_PASS == 8192);
static_assert(HT_CELL_BYTES == 16);

/// One benchmark cell: UInt64 payload scattered to `fanout` exact-sized partitions, in-tree policy
/// (SWWC iff fanout >= SWWC_MIN_FANOUT; 8 bytes support SWWC), batch sized per the in-tree constants.
struct ReferenceFixture
{
    size_t fanout;
    size_t n;
    bool use_swwc;
    UInt32 shift;
    UInt32 mask;
    PaddedPODArray<UInt64> keys;
    PaddedPODArray<UInt16> pids;
    PaddedPODArray<UInt16> pids_out;
    std::vector<PaddedPODArray<char>> parts;
    std::vector<char *> bases;
    ScatterScratch scratch;

    explicit ReferenceFixture(size_t fanout_)
        : fanout(fanout_)
        , n(scatterBatchRowsTarget(fanout_))
        , use_swwc(fanout_ >= SWWC_MIN_FANOUT && widthSupportsSwwc(8))
        , shift(static_cast<UInt32>(32 - std::countr_zero(fanout_)))
        , mask(static_cast<UInt32>(fanout_ - 1))
    {
        keys.resize(n);
        pids.resize(n);
        pids_out.resize(n);

        /// Fixed seed: identical inputs across repetitions, process runs, and (in U4) arms.
        pcg64 rng(42);
        for (size_t i = 0; i < n; ++i)
            keys[i] = rng();
        for (size_t i = 0; i < n; ++i)
            pids[i] = static_cast<UInt16>((routeWord(keys[i]) >> shift) & mask);

        /// Untimed: histogram + exact-sized per-partition allocation (the in-tree barriers 1-2).
        std::vector<size_t> counts(fanout, 0);
        for (size_t i = 0; i < n; ++i)
            ++counts[pids[i]];
        parts.resize(fanout);
        bases.resize(fanout);
        for (size_t p = 0; p < fanout; ++p)
        {
            parts[p].resize(counts[p] * sizeof(UInt64));
            bases[p] = parts[p].data();
        }

        scratch.init(fanout, use_swwc);
        verify();
    }

    void seedAll()
    {
        for (size_t p = 0; p < fanout; ++p)
            scratch.seed(p, bases[p]);
    }

    void runPidMode()
    {
        seedAll();
        scatterOne<8>(RouteFromPids{pids.data()}, reinterpret_cast<const char *>(keys.data()), n, use_swwc, scratch);
        scratch.drain();
    }

    void runKeyMode()
    {
        seedAll();
        scatterOne<8>(
            RouteFromKey<8>{reinterpret_cast<const char *>(keys.data()), shift, mask, pids_out.data()},
            reinterpret_cast<const char *>(keys.data()),
            n,
            use_swwc,
            scratch);
        scratch.drain();
    }

    /// Correctness oracle (pre-registered soundness check S4): per-partition count and value-sum of
    /// the scattered output must match a scalar reference; both routing modes must agree; the key
    /// mode must emit exactly the pids the fixture derived from routeWord.
    void verify()
    {
        std::vector<size_t> expected_count(fanout, 0);
        std::vector<UInt64> expected_sum(fanout, 0);
        for (size_t i = 0; i < n; ++i)
        {
            ++expected_count[pids[i]];
            expected_sum[pids[i]] += keys[i];
        }

        auto check = [&](const char * mode)
        {
            for (size_t p = 0; p < fanout; ++p)
            {
                const size_t count = parts[p].size() / sizeof(UInt64);
                if (count != expected_count[p])
                    throw std::runtime_error(std::string("scatter reference oracle: bad count in mode ") + mode);
                UInt64 sum = 0;
                for (size_t i = 0; i < count; ++i)
                {
                    UInt64 v = 0;
                    std::memcpy(&v, parts[p].data() + i * sizeof(UInt64), sizeof(v));
                    sum += v;
                }
                if (sum != expected_sum[p])
                    throw std::runtime_error(std::string("scatter reference oracle: bad content sum in mode ") + mode);
            }
        };

        runPidMode();
        check("pid8");
        runKeyMode();
        check("key8");
        for (size_t i = 0; i < n; ++i)
            if (pids_out[i] != pids[i])
                throw std::runtime_error("scatter reference oracle: key mode emitted wrong pids");
    }
};

/// Module arms (U1+): the same cells driven through DB::ColumnsScatter. The Layer-0 arms write into
/// the SAME preallocated partition buffers as the reference arm (identical timed-region definition:
/// seed + scatter + drain, allocation untimed) — a kernel-parity measurement. The Layer-1 arm times
/// the full one-shot `scatter` call (dispatch + normalization gate + exact allocation + kernel +
/// result teardown) — a definitionally different, informational cell (PREREG P-U1-2).
struct ModuleFixture
{
    ReferenceFixture & ref; /// borrowed; the registering lambda co-captures the owning shared_ptr
    PaddedPODArray<UInt32> pids32;
    ColumnsScatter::ScatterScratch scratch;
    /// Layer-1 inputs: a real column mirroring the reference keys + precomputed shard counts.
    MutableColumnPtr source_column;
    std::vector<UInt32> counts32;

    explicit ModuleFixture(ReferenceFixture & ref_) : ref(ref_)
    {
        pids32.resize(ref.n);
        for (size_t i = 0; i < ref.n; ++i)
            pids32[i] = ref.pids[i];
        scratch.init(ref.fanout, ref.use_swwc);

        auto column = ColumnUInt64::create();
        auto raw = column->insertRawUninitialized(ref.n);
        std::memcpy(raw.data(), ref.keys.data(), ref.n * sizeof(UInt64));
        source_column = std::move(column);
        counts32.assign(ref.fanout, 0);
        for (size_t i = 0; i < ref.n; ++i)
            ++counts32[ref.pids[i]];

        verify();
    }

    void seedAll()
    {
        for (size_t p = 0; p < ref.fanout; ++p)
            scratch.seed(p, ref.bases[p]);
    }

    void runPid16()
    {
        seedAll();
        ColumnsScatter::scatterPidChunk(8, ref.pids.data(), reinterpret_cast<const char *>(ref.keys.data()), ref.n, ref.use_swwc, scratch);
        scratch.drain();
    }

    void runPid32()
    {
        seedAll();
        ColumnsScatter::scatterPidChunk(8, pids32.data(), reinterpret_cast<const char *>(ref.keys.data()), ref.n, ref.use_swwc, scratch);
        scratch.drain();
    }

    void runKey16()
    {
        seedAll();
        ColumnsScatter::scatterKeyChunk(
            8, reinterpret_cast<const char *>(ref.keys.data()), ref.n, ref.shift, ref.mask, ref.pids_out.data(), ref.use_swwc, scratch);
        scratch.drain();
    }

    MutableColumns runLayer1()
    {
        const IColumn * source = source_column.get();
        std::span<const UInt16> pid_span(ref.pids.data(), ref.n);
        return ColumnsScatter::scatter(
            std::span<const IColumn * const>(&source, 1),
            std::span<const std::span<const UInt16>>(&pid_span, 1),
            ref.fanout,
            std::span<const UInt32>(counts32.data(), counts32.size()));
    }

    /// Same oracle as the reference arm, applied to every module path.
    void verify()
    {
        std::vector<size_t> expected_count(ref.fanout, 0);
        std::vector<UInt64> expected_sum(ref.fanout, 0);
        for (size_t i = 0; i < ref.n; ++i)
        {
            ++expected_count[ref.pids[i]];
            expected_sum[ref.pids[i]] += ref.keys[i];
        }

        auto check_parts = [&](const char * mode)
        {
            for (size_t p = 0; p < ref.fanout; ++p)
            {
                const size_t count = ref.parts[p].size() / sizeof(UInt64);
                UInt64 sum = 0;
                for (size_t i = 0; i < count; ++i)
                {
                    UInt64 v = 0;
                    std::memcpy(&v, ref.parts[p].data() + i * sizeof(UInt64), sizeof(v));
                    sum += v;
                }
                if (count != expected_count[p] || sum != expected_sum[p])
                    throw std::runtime_error(std::string("scatter module oracle: bad partition in mode ") + mode);
            }
        };

        runPid16();
        check_parts("mod0_pid16");
        runPid32();
        check_parts("mod0_pid32");
        runKey16();
        check_parts("mod0_key16");
        for (size_t i = 0; i < ref.n; ++i)
            if (ref.pids_out[i] != ref.pids[i])
                throw std::runtime_error("scatter module oracle: key mode emitted wrong pids");

        auto shards = runLayer1();
        for (size_t p = 0; p < ref.fanout; ++p)
        {
            const auto raw = shards[p]->getRawData();
            const size_t count = raw.size() / sizeof(UInt64);
            UInt64 sum = 0;
            for (size_t i = 0; i < count; ++i)
            {
                UInt64 v = 0;
                std::memcpy(&v, raw.data() + i * sizeof(UInt64), sizeof(v));
                sum += v;
            }
            if (count != expected_count[p] || sum != expected_sum[p])
                throw std::runtime_error("scatter module oracle: bad shard in mode mod1_layer1");
        }
    }
};

std::vector<std::shared_ptr<ReferenceFixture>> registerReferenceBenchmarks()
{
    static constexpr size_t fanouts[] = {64, 256, 2048, 8192};
    std::vector<std::shared_ptr<ReferenceFixture>> fixtures;
    for (size_t fanout : fanouts)
    {
        auto fixture = std::make_shared<ReferenceFixture>(fanout);
        fixtures.push_back(fixture);
        benchmark::RegisterBenchmark(
            ("BM_ref_pid8/F" + std::to_string(fanout)).c_str(),
            [fixture](benchmark::State & state)
            {
                for (auto _ : state)
                {
                    fixture->runPidMode();
                    benchmark::ClobberMemory();
                }
                state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * fixture->n * sizeof(UInt64));
                state.counters["rows"] = static_cast<double>(fixture->n);
                state.counters["swwc"] = fixture->use_swwc ? 1 : 0;
            });
        benchmark::RegisterBenchmark(
            ("BM_ref_key8/F" + std::to_string(fanout)).c_str(),
            [fixture](benchmark::State & state)
            {
                for (auto _ : state)
                {
                    fixture->runKeyMode();
                    benchmark::ClobberMemory();
                }
                state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * fixture->n * sizeof(UInt64));
                state.counters["rows"] = static_cast<double>(fixture->n);
                state.counters["swwc"] = fixture->use_swwc ? 1 : 0;
            });

        auto module_fixture = std::make_shared<ModuleFixture>(*fixture);
        auto register_module_cell = [&](const char * name, auto run)
        {
            benchmark::RegisterBenchmark(
                (std::string(name) + "/F" + std::to_string(fanout)).c_str(),
                [fixture, module_fixture, run](benchmark::State & state)
                {
                    for (auto _ : state)
                    {
                        run(*module_fixture);
                        benchmark::ClobberMemory();
                    }
                    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * fixture->n * sizeof(UInt64));
                    state.counters["rows"] = static_cast<double>(fixture->n);
                    state.counters["swwc"] = fixture->use_swwc ? 1 : 0;
                });
        };
        register_module_cell("BM_mod0_pid16", [](ModuleFixture & f) { f.runPid16(); });
        register_module_cell("BM_mod0_pid32", [](ModuleFixture & f) { f.runPid32(); });
        register_module_cell("BM_mod0_key16", [](ModuleFixture & f) { f.runKey16(); });
        register_module_cell(
            "BM_mod1_full",
            [](ModuleFixture & f)
            {
                auto shards = f.runLayer1();
                benchmark::DoNotOptimize(shards.data());
            });
    }
    return fixtures;
}

/// U2 String cells: the variable-length Layer-0 kernel at fixed row length L, same fanouts and
/// batch-row sizing as the reference. Bytes counted = chars + offsets actually written
/// ((L + 8) per row). Timed region = seed + scatterStringChunk (no staging to drain); byte
/// histogram and destination allocation are untimed, mirroring the fixed-width cells.
struct StringFixture
{
    size_t fanout;
    size_t n;
    size_t length;
    PaddedPODArray<char> chars;
    PaddedPODArray<UInt64> offsets;
    PaddedPODArray<UInt16> pids;
    std::vector<PaddedPODArray<char>> chars_destination; /// one per shard: overflow-15 tolerant each
    PaddedPODArray<UInt64> offsets_destination;
    std::vector<char *> chars_bases;
    std::vector<UInt64 *> offsets_bases;
    ColumnsScatter::StringScatterState state;

    StringFixture(size_t fanout_, size_t length_)
        : fanout(fanout_)
        , n(ScatterReference::scatterBatchRowsTarget(fanout_))
        , length(length_)
    {
        chars.resize(n * length);
        offsets.resize(n);
        pids.resize(n);
        pcg64 rng(42);
        for (auto & byte : chars)
            byte = static_cast<char>(rng());
        for (size_t i = 0; i < n; ++i)
            offsets[i] = (i + 1) * length;
        const UInt32 shift = static_cast<UInt32>(32 - std::countr_zero(fanout));
        const UInt32 mask = static_cast<UInt32>(fanout - 1);
        for (size_t i = 0; i < n; ++i)
            pids[i] = static_cast<UInt16>((ScatterReference::routeWord(rng()) >> shift) & mask);

        /// Untimed: byte/row histograms + exact destination carving at per-shard offsets.
        std::vector<UInt64> byte_counts(fanout, 0);
        std::vector<UInt64> row_counts(fanout, 0);
        for (size_t i = 0; i < n; ++i)
        {
            byte_counts[pids[i]] += length;
            ++row_counts[pids[i]];
        }
        chars_destination.resize(fanout);
        offsets_destination.resize(n);
        chars_bases.resize(fanout);
        offsets_bases.resize(fanout);
        UInt64 row_prefix = 0;
        for (size_t p = 0; p < fanout; ++p)
        {
            /// Per-shard chars allocations: the kernel's overflow-15 copies require each shard's
            /// region to be independently overflow-tolerant (see the Layer-0 contract).
            chars_destination[p].resize(byte_counts[p]);
            chars_bases[p] = chars_destination[p].data();
            offsets_bases[p] = offsets_destination.data() + row_prefix;
            row_prefix += row_counts[p];
        }
        state.init(fanout);
        verify();
    }

    void run()
    {
        for (size_t p = 0; p < fanout; ++p)
            state.seed(p, chars_bases[p], offsets_bases[p], 0);
        ColumnsScatter::scatterStringChunk(chars.data(), offsets.data(), pids.data(), n, state);
    }

    /// Scalar-reference oracle over both output streams.
    void verify()
    {
        run();
        std::vector<UInt64> row_cursor(fanout, 0);
        std::vector<UInt64> byte_cursor(fanout, 0);
        for (size_t i = 0; i < n; ++i)
        {
            const size_t p = pids[i];
            const char * expected = chars.data() + i * length;
            const char * actual = chars_bases[p] + byte_cursor[p];
            if (std::memcmp(expected, actual, length) != 0)
                throw std::runtime_error("string scatter oracle: bad chars");
            byte_cursor[p] += length;
            if (offsets_bases[p][row_cursor[p]] != byte_cursor[p])
                throw std::runtime_error("string scatter oracle: bad rebased offset");
            ++row_cursor[p];
        }
    }
};

/// U2 Nullable(UInt64) cell: two fixed streams (width-1 null map + width-8 payload) through the
/// module chunk kernel, driven by one pid stream. Bytes counted = 9 per row.
struct NullableFixture
{
    ReferenceFixture & ref; /// borrowed; the registering lambda co-captures the owning shared_ptr
    PaddedPODArray<char> null_bytes;
    PaddedPODArray<char> null_destination;
    std::vector<char *> null_bases;
    ColumnsScatter::ScatterScratch null_scratch;
    ColumnsScatter::ScatterScratch payload_scratch;

    explicit NullableFixture(ReferenceFixture & ref_) : ref(ref_)
    {
        pcg64 rng(43);
        null_bytes.resize(ref.n);
        for (auto & byte : null_bytes)
            byte = (rng() % 4) == 0;
        null_destination.resize(ref.n);
        null_bases.resize(ref.fanout);
        std::vector<size_t> counts(ref.fanout, 0);
        for (size_t i = 0; i < ref.n; ++i)
            ++counts[ref.pids[i]];
        size_t prefix = 0;
        for (size_t p = 0; p < ref.fanout; ++p)
        {
            null_bases[p] = null_destination.data() + prefix;
            prefix += counts[p];
        }
        null_scratch.init(ref.fanout, ref.use_swwc);
        payload_scratch.init(ref.fanout, ref.use_swwc);
    }

    void run()
    {
        for (size_t p = 0; p < ref.fanout; ++p)
        {
            null_scratch.seed(p, null_bases[p]);
            payload_scratch.seed(p, ref.bases[p]);
        }
        ColumnsScatter::scatterPidChunk(1, ref.pids.data(), null_bytes.data(), ref.n, ref.use_swwc, null_scratch);
        ColumnsScatter::scatterPidChunk(
            8, ref.pids.data(), reinterpret_cast<const char *>(ref.keys.data()), ref.n, ref.use_swwc, payload_scratch);
        null_scratch.drain();
        payload_scratch.drain();
    }
};

void registerStringBenchmarks()
{
    static constexpr size_t fanouts[] = {64, 256, 2048, 8192};
    for (size_t fanout : fanouts)
    {
        for (size_t length : {8uz, 32uz})
        {
            auto fixture = std::make_shared<StringFixture>(fanout, length);
            benchmark::RegisterBenchmark(
                ("BM_mod0_str_L" + std::to_string(length) + "/F" + std::to_string(fanout)).c_str(),
                [fixture](benchmark::State & state)
                {
                    for (auto _ : state)
                    {
                        fixture->run();
                        benchmark::ClobberMemory();
                    }
                    state.SetBytesProcessed(
                        static_cast<int64_t>(state.iterations()) * fixture->n * (fixture->length + sizeof(UInt64)));
                    state.counters["rows"] = static_cast<double>(fixture->n);
                });
        }
    }
}

/// U3: fallback throughput documentation cells (exotic leaf types are exempt from the bandwidth
/// gate; their legacy-scatter cost is measured and documented instead). Timed region = the full
/// Layer-1 call (the fallback owns allocation), bytes = a nominal 8 per row for comparability.
struct FallbackFixture
{
    size_t fanout;
    size_t n;
    MutableColumnPtr column;
    PaddedPODArray<UInt16> pids;

    FallbackFixture(size_t fanout_, const char * type_name) : fanout(fanout_), n(256 << 10)
    {
        auto type = DataTypeFactory::instance().get(type_name);
        column = type->createColumn();
        pcg64 rng(44);
        for (size_t i = 0; i < n; ++i)
        {
            if (rng() % 3 == 0)
                column->insert(Field("v_" + std::to_string(i % 97)));
            else
                column->insert(Field(static_cast<UInt64>(rng())));
        }
        pids.resize(n);
        const UInt32 shift = static_cast<UInt32>(32 - std::countr_zero(fanout));
        const UInt32 mask = static_cast<UInt32>(fanout - 1);
        for (size_t i = 0; i < n; ++i)
            pids[i] = static_cast<UInt16>((ScatterReference::routeWord(rng()) >> shift) & mask);
    }

    MutableColumns run()
    {
        const IColumn * source = column.get();
        std::span<const UInt16> pid_span(pids.data(), n);
        return ColumnsScatter::scatter(
            std::span<const IColumn * const>(&source, 1), std::span<const std::span<const UInt16>>(&pid_span, 1), fanout);
    }
};

/// Settling cell for the Nullable composite basis: the SAME alternating two-pass shape as
/// `BM_mod0_null8` (width-1 pass + width-8 pass per iteration) built from the U0-frozen REFERENCE
/// kernels — if this shows the same cost as the module cell, the passes are tax-free and the
/// isolated-pass composite (`t_ref_pid1` + `t_ref_pid8`) is simply an optimistic basis at
/// cache-boundary fanouts (inter-pass interference).
struct NullableRefFixture
{
    NullableFixture & mod; /// borrowed; registering lambda co-captures the owning shared_ptr
    ScatterScratch null_scratch;
    ScatterScratch payload_scratch;

    explicit NullableRefFixture(NullableFixture & mod_) : mod(mod_)
    {
        null_scratch.init(mod.ref.fanout, mod.ref.use_swwc);
        payload_scratch.init(mod.ref.fanout, mod.ref.use_swwc);
    }

    void run()
    {
        for (size_t p = 0; p < mod.ref.fanout; ++p)
        {
            null_scratch.seed(p, mod.null_bases[p]);
            payload_scratch.seed(p, mod.ref.bases[p]);
        }
        scatterOne<1>(RouteFromPids{mod.ref.pids.data()}, mod.null_bytes.data(), mod.ref.n, mod.ref.use_swwc, null_scratch);
        scatterOne<8>(
            RouteFromPids{mod.ref.pids.data()},
            reinterpret_cast<const char *>(mod.ref.keys.data()),
            mod.ref.n,
            mod.ref.use_swwc,
            payload_scratch);
        null_scratch.drain();
        payload_scratch.drain();
    }
};


/// ------------------------------------------------------------------------------------------------
/// D-0008 thread sweep: parity under contention. A persistent fork-join pool runs T workers per
/// iteration; each worker scatters the SAME shared input into its own private destinations with its
/// own scratch (same aggregate traffic as the join's shared-partition writes, without false sharing
/// at partition boundaries). Aggregate bytes/s reported (per-thread bytes summed by the driver).
/// ------------------------------------------------------------------------------------------------
class ForkJoinPool
{
public:
    explicit ForkJoinPool(size_t threads_) : threads(threads_), start_barrier(threads_ + 1), end_barrier(threads_ + 1)
    {
        for (size_t t = 0; t < threads; ++t)
            workers.emplace_back(
                [this, t]
                {
                    while (true)
                    {
                        start_barrier.arrive_and_wait();
                        if (stop.load(std::memory_order_acquire))
                            return;
                        job(t);
                        end_barrier.arrive_and_wait();
                    }
                });
    }

    ~ForkJoinPool()
    {
        stop.store(true, std::memory_order_release);
        start_barrier.arrive_and_wait();
        for (auto & worker : workers)
            worker.join();
    }

    void run(const std::function<void(size_t)> & job_)
    {
        job = job_;
        start_barrier.arrive_and_wait();
        end_barrier.arrive_and_wait();
    }

private:
    size_t threads;
    std::barrier<> start_barrier;
    std::barrier<> end_barrier;
    std::function<void(size_t)> job;
    std::atomic<bool> stop{false};
    std::vector<std::thread> workers;
};

/// The ONE reference-arm dispatch for a routed fixed-width scatter, shared by `WidthFixture` and
/// `MtFixture` (review U4-simplicity-2: two hand-maintained copies of this switch would let the
/// Gate-1 basis silently drift). The U0-frozen verbatim `scatterOne` handles the in-tree
/// instantiated widths; any other width takes the in-tree generic path (no SWWC; per-row copy
/// with a RUNTIME width), mirroring the default branch of the in-tree pid dispatch — the width
/// must not be a literal in that call or the compiler constant-propagates it into an inline copy
/// the real call sites never get (METHODOLOGY L0015).
template <typename Route>
void referenceScatterByWidth(const Route & route, const char * data, size_t n, size_t width, bool use_swwc, ScatterScratch & scratch)
{
    switch (width)
    {
        case 1: scatterOne<1>(route, data, n, use_swwc, scratch); break;
        case 2: scatterOne<2>(route, data, n, use_swwc, scratch); break;
        case 4: scatterOne<4>(route, data, n, use_swwc, scratch); break;
        case 8: scatterOne<8>(route, data, n, use_swwc, scratch); break;
        case 16: scatterOne<16>(route, data, n, use_swwc, scratch); break;
        default:
            scatterDirectGeneric(route, data, n, width, scratch.cursors.data());
            return; /// direct cursor writes — nothing staged to drain
    }
    scratch.drain();
}

/// One family cell: shared input, per-worker destinations/scratch for up to `threads` workers.
/// `kind` selects the arm pair; the ref arm uses the U0-frozen verbatim kernels, the mod arm the
/// module exports — identical shapes.
struct MtFixture
{
    enum class Kind : uint8_t { Pid8, Key8, W1, W4, W7, W16, W32, W33, W48, StrL8, Null8 };

    Kind kind;
    size_t fanout;
    size_t threads;
    size_t n;
    size_t width;
    bool use_swwc;
    UInt32 shift;
    UInt32 mask;
    PaddedPODArray<UInt64> keys;
    PaddedPODArray<char> data;
    PaddedPODArray<UInt16> pids;
    PaddedPODArray<char> null_bytes;
    PaddedPODArray<UInt64> string_offsets;
    std::vector<size_t> counts;

    struct Worker
    {
        std::vector<PaddedPODArray<char>> parts;         /// per shard (fixed-width / chars payloads)
        std::vector<PaddedPODArray<char>> null_parts;    /// Null8: width-1 stream
        PaddedPODArray<UInt64> offsets_out;              /// StrL8: offsets stream
        PaddedPODArray<UInt16> pids_out;                 /// Key8
        std::vector<char *> bases;
        std::vector<char *> null_bases;
        std::vector<UInt64 *> offsets_bases;
        ScatterScratch ref_scratch;
        ScatterScratch ref_scratch2;
        ColumnsScatter::ScatterScratch mod_scratch;
        ColumnsScatter::ScatterScratch mod_scratch2;
        ColumnsScatter::StringScatterState string_state;
    };
    std::vector<Worker> workers;

    /// One shared pool per thread count (five total across every MT cell) — per-fixture pools would
    /// park thousands of idle threads at registration time.
    static ForkJoinPool & poolFor(size_t threads)
    {
        static std::map<size_t, std::unique_ptr<ForkJoinPool>> pools;
        auto it = pools.find(threads);
        if (it == pools.end())
            it = pools.emplace(threads, std::make_unique<ForkJoinPool>(threads)).first;
        return *it->second;
    }

    MtFixture(Kind kind_, size_t fanout_, size_t threads_, size_t n_override = 0)
        : kind(kind_)
        , fanout(fanout_)
        , threads(threads_)
        , n(n_override ? n_override : scatterBatchRowsTarget(fanout_))
        , width(kindWidth(kind_))
        , use_swwc(fanout_ >= SWWC_MIN_FANOUT && widthSupportsSwwc(width))
        , shift(static_cast<UInt32>(32 - std::countr_zero(fanout_)))
        , mask(static_cast<UInt32>(fanout_ - 1))
    {
        pcg64 rng(50);
        keys.resize(n);
        for (auto & key : keys)
            key = rng();
        pids.resize(n);
        for (size_t i = 0; i < n; ++i)
            pids[i] = static_cast<UInt16>((routeWord(keys[i]) >> shift) & mask);
        counts.assign(fanout, 0);
        for (size_t i = 0; i < n; ++i)
            ++counts[pids[i]];

        if (kind == Kind::Pid8 || kind == Kind::Key8)
        {
            data.resize(n * 8);
            std::memcpy(data.data(), reinterpret_cast<const char *>(keys.data()), n * 8);
        }
        else if (kind == Kind::StrL8)
        {
            data.resize(n * 8);
            std::memcpy(data.data(), reinterpret_cast<const char *>(keys.data()), n * 8);
            string_offsets.resize(n);
            for (size_t i = 0; i < n; ++i)
                string_offsets[i] = (i + 1) * 8;
        }
        else
        {
            data.resize(n * width);
            for (auto & byte : data)
                byte = static_cast<char>(rng());
        }
        if (kind == Kind::Null8)
        {
            null_bytes.resize(n);
            for (auto & byte : null_bytes)
                byte = (rng() % 4) == 0;
        }

        workers.resize(threads);
        for (auto & worker : workers)
        {
            worker.parts.resize(fanout);
            worker.bases.resize(fanout);
            for (size_t p = 0; p < fanout; ++p)
            {
                worker.parts[p].resize(counts[p] * width);
                worker.bases[p] = worker.parts[p].data();
            }
            worker.ref_scratch.init(fanout, use_swwc);
            worker.mod_scratch.init(fanout, use_swwc);
            if (kind == Kind::Key8)
                worker.pids_out.resize(n);
            if (kind == Kind::Null8)
            {
                worker.null_parts.resize(fanout);
                worker.null_bases.resize(fanout);
                for (size_t p = 0; p < fanout; ++p)
                {
                    worker.null_parts[p].resize(counts[p]);
                    worker.null_bases[p] = worker.null_parts[p].data();
                }
                worker.ref_scratch2.init(fanout, use_swwc);
                worker.mod_scratch2.init(fanout, use_swwc);
            }
            if (kind == Kind::StrL8)
            {
                worker.offsets_out.resize(n);
                worker.offsets_bases.resize(fanout);
                size_t row_prefix = 0;
                for (size_t p = 0; p < fanout; ++p)
                {
                    worker.offsets_bases[p] = worker.offsets_out.data() + row_prefix;
                    row_prefix += counts[p];
                }
                worker.string_state.init(fanout);
            }
        }
        verify();
    }

    static size_t kindWidth(Kind kind_)
    {
        switch (kind_)
        {
            case Kind::W1: return 1;
            case Kind::W4: return 4;
            case Kind::W7: return 7;
            case Kind::W16: return 16;
            case Kind::W32: return 32;
            case Kind::W33: return 33;
            case Kind::W48: return 48;
            /// Explicit so a new Kind trips -Wswitch here, not silently width 8 (review U4-simplicity-2).
            case Kind::Pid8:
            case Kind::Key8:
            case Kind::StrL8:
            case Kind::Null8: return 8;
        }
        UNREACHABLE();
    }

    /// Bytes genuinely moved per WORKER per iteration (for aggregate accounting).
    size_t bytesPerWorker() const
    {
        if (kind == Kind::StrL8)
            return n * 16;
        if (kind == Kind::Null8)
            return n * 9;
        return n * width;
    }

    void runArm(bool module_arm)
    {
        poolFor(threads).run(
            [this, module_arm](size_t t)
            {
                Worker & worker = workers[t];
                if (module_arm)
                    runModWorker(worker);
                else
                    runRefWorker(worker);
            });
    }

    void runRefWorker(Worker & worker)
    {
        const RouteFromPids route{pids.data()};
        switch (kind)
        {
            case Kind::Pid8:
            case Kind::W1:
            case Kind::W4:
            case Kind::W7:
            case Kind::W16:
            case Kind::W32:
            case Kind::W33:
            case Kind::W48:
                seedRef(worker.ref_scratch, worker.bases);
                referenceScatterByWidth(route, data.data(), n, width, use_swwc, worker.ref_scratch);
                break;
            case Kind::Key8:
                seedRef(worker.ref_scratch, worker.bases);
                referenceScatterByWidth(
                    RouteFromKey<8>{data.data(), shift, mask, worker.pids_out.data()}, data.data(), n, width, use_swwc,
                    worker.ref_scratch);
                break;
            case Kind::StrL8:
                /// No reference arm exists for the two-stream String job (no in-tree var-len kernel;
                /// it would run the identical module kernel) — only the mod arm is registered, and
                /// String parity is established by the 1T literal-basis cells (review U4-simplicity-4).
                throw std::runtime_error("MtFixture: StrL8 has no reference arm");
            case Kind::Null8:
                seedRef(worker.ref_scratch, worker.null_bases);
                seedRef(worker.ref_scratch2, worker.bases);
                scatterOne<1>(route, null_bytes.data(), n, use_swwc, worker.ref_scratch);
                scatterOne<8>(route, data.data(), n, use_swwc, worker.ref_scratch2);
                worker.ref_scratch.drain();
                worker.ref_scratch2.drain();
                break;
        }
    }

    void runModWorker(Worker & worker)
    {
        switch (kind)
        {
            case Kind::Pid8:
            case Kind::W1:
            case Kind::W4:
            case Kind::W7:
            case Kind::W16:
            case Kind::W32:
            case Kind::W33:
            case Kind::W48:
                seedMod(worker.mod_scratch, worker.bases);
                ColumnsScatter::scatterPidChunk(width, pids.data(), data.data(), n, use_swwc, worker.mod_scratch);
                worker.mod_scratch.drain();
                break;
            case Kind::Key8:
                seedMod(worker.mod_scratch, worker.bases);
                ColumnsScatter::scatterKeyChunk(8, data.data(), n, shift, mask, worker.pids_out.data(), use_swwc, worker.mod_scratch);
                worker.mod_scratch.drain();
                break;
            case Kind::StrL8:
                seedString(worker);
                ColumnsScatter::scatterStringChunk(data.data(), string_offsets.data(), pids.data(), n, worker.string_state);
                break;
            case Kind::Null8:
                seedMod(worker.mod_scratch, worker.null_bases);
                seedMod(worker.mod_scratch2, worker.bases);
                ColumnsScatter::scatterPidChunk(1, pids.data(), null_bytes.data(), n, use_swwc, worker.mod_scratch);
                ColumnsScatter::scatterPidChunk(8, pids.data(), data.data(), n, use_swwc, worker.mod_scratch2);
                worker.mod_scratch.drain();
                worker.mod_scratch2.drain();
                break;
        }
    }

    void seedRef(ScatterScratch & scratch, const std::vector<char *> & shard_bases)
    {
        for (size_t p = 0; p < fanout; ++p)
            scratch.seed(p, shard_bases[p]);
    }

    void seedMod(ColumnsScatter::ScatterScratch & scratch, const std::vector<char *> & shard_bases)
    {
        for (size_t p = 0; p < fanout; ++p)
            scratch.seed(p, shard_bases[p]);
    }

    void seedString(Worker & worker)
    {
        for (size_t p = 0; p < fanout; ++p)
            worker.string_state.seed(p, worker.bases[p], worker.offsets_bases[p], 0);
    }

    /// In-harness oracle (review U4-corr-1): run each arm once on worker 0 and content-check every
    /// output stream against a scalar walk of the shared input. Destinations are poisoned before
    /// each arm so the second check proves genuine writes (review U4-corr-2). Worker state is
    /// re-seeded by every run, so the timed runs start from the same state as without the oracle.
    void verify()
    {
        Worker & worker = workers[0];
        if (kind != Kind::StrL8) /// StrL8 registers only the module arm (see runRefWorker)
        {
            poisonWorker(worker);
            runRefWorker(worker);
            checkWorker(worker, "ref");
        }
        poisonWorker(worker);
        runModWorker(worker);
        checkWorker(worker, "mod");
    }

    void poisonWorker(Worker & worker) const
    {
        for (auto & part : worker.parts)
            if (!part.empty())
                std::memset(part.data(), 0xAA, part.size());
        for (auto & part : worker.null_parts)
            if (!part.empty())
                std::memset(part.data(), 0xAA, part.size());
        if (!worker.pids_out.empty())
            std::memset(worker.pids_out.data(), 0xAA, worker.pids_out.size() * sizeof(UInt16));
        if (!worker.offsets_out.empty())
            std::memset(worker.offsets_out.data(), 0xAA, worker.offsets_out.size() * sizeof(UInt64));
    }

    void checkWorker(const Worker & worker, const char * arm) const
    {
        auto fail = [&](const char * what)
        { throw std::runtime_error(std::string("MtFixture oracle (") + arm + "): " + what); };
        std::vector<size_t> row_cursor(fanout, 0);
        for (size_t i = 0; i < n; ++i)
        {
            const size_t p = pids[i];
            const size_t r = row_cursor[p];
            if (std::memcmp(worker.parts[p].data() + r * width, data.data() + i * width, width) != 0)
                fail(kind == Kind::StrL8 ? "bad chars" : "bad payload");
            if (kind == Kind::StrL8 && worker.offsets_bases[p][r] != (r + 1) * 8)
                fail("bad rebased offset");
            if (kind == Kind::Key8 && worker.pids_out[i] != pids[i])
                fail("bad emitted pid");
            if (kind == Kind::Null8 && worker.null_parts[p][r] != null_bytes[i])
                fail("bad null byte");
            ++row_cursor[p];
        }
    }
};

void registerThreadSweepBenchmarks()
{
    struct Family
    {
        const char * name;
        MtFixture::Kind kind;
    };
    static constexpr Family families[] = {
        {"pid8", MtFixture::Kind::Pid8},
        {"key8", MtFixture::Kind::Key8},
        {"w1", MtFixture::Kind::W1},
        {"w4", MtFixture::Kind::W4},
        {"w7", MtFixture::Kind::W7},
        {"w16", MtFixture::Kind::W16},
        {"w32", MtFixture::Kind::W32},
        {"w33", MtFixture::Kind::W33},
        {"w48", MtFixture::Kind::W48},
        {"strL8", MtFixture::Kind::StrL8},
        {"null8", MtFixture::Kind::Null8},
    };
    for (size_t fanout : {64uz, 256uz, 8192uz})
    {
        for (const auto & family : families)
        {
            for (size_t threads : {1uz, 16uz, 32uz, 64uz, 96uz})
            {
                /// Constructed lazily on the cell's first use (review U4-simplicity-1): eager
                /// construction of the full matrix at registration cost ~60 GB and ~24 s per
                /// invocation, including `--benchmark_list_tests`. The holder is shared by both
                /// arms of the pair so they see the same input and destinations.
                auto holder = std::make_shared<std::unique_ptr<MtFixture>>();
                const MtFixture::Kind kind = family.kind;
                for (bool module_arm : {false, true})
                {
                    /// String has no distinct in-tree arm (see `runRefWorker`); register mod only.
                    if (!module_arm && kind == MtFixture::Kind::StrL8)
                        continue;
                    benchmark::RegisterBenchmark(
                        (std::string("BM_mt_") + (module_arm ? "mod_" : "ref_") + family.name + "/F" + std::to_string(fanout) + "/T"
                         + std::to_string(threads))
                            .c_str(),
                        [holder, kind, fanout, threads, module_arm](benchmark::State & state)
                        {
                            if (!*holder)
                                *holder = std::make_unique<MtFixture>(kind, fanout, threads);
                            MtFixture & fixture = **holder;
                            for (auto _ : state)
                            {
                                fixture.runArm(module_arm);
                                benchmark::ClobberMemory();
                            }
                            state.SetBytesProcessed(
                                static_cast<int64_t>(state.iterations()) * fixture.bytesPerWorker() * fixture.threads);
                            state.counters["threads"] = static_cast<double>(fixture.threads);
                        })
                        /// The driver thread only waits on barriers; its CPU time is meaningless —
                        /// aggregate bandwidth must be computed over wall time.
                        ->UseRealTime();
                }
            }
        }
    }
}

/// R8: long-iteration variants of the MT cells (~16.8M rows per worker), sized so one iteration
/// runs for hundreds of ms. Same arms and oracle as the normal MT cells, only `n` is overridden.
void registerBigThreadSweepBenchmarks()
{
    static constexpr size_t BIG_ROWS = 16 << 20;
    struct BigCell
    {
        const char * name;
        MtFixture::Kind kind;
        size_t fanout;
    };
    static constexpr BigCell cells[] = {
        {"w16big", MtFixture::Kind::W16, 8192},
        {"w32big", MtFixture::Kind::W32, 8192},
        {"w33big", MtFixture::Kind::W33, 8192},
        {"w48big", MtFixture::Kind::W48, 8192},
        {"w48big", MtFixture::Kind::W48, 64},
    };
    for (const auto & cell : cells)
    {
        for (size_t threads : {1uz, 64uz})
        {
            auto holder = std::make_shared<std::unique_ptr<MtFixture>>();
            const MtFixture::Kind kind = cell.kind;
            const size_t fanout = cell.fanout;
            for (bool module_arm : {false, true})
            {
                benchmark::RegisterBenchmark(
                    (std::string("BM_mt_") + (module_arm ? "mod_" : "ref_") + cell.name + "/F" + std::to_string(fanout) + "/T"
                     + std::to_string(threads))
                        .c_str(),
                    [holder, kind, fanout, threads, module_arm](benchmark::State & state)
                    {
                        if (!*holder)
                            *holder = std::make_unique<MtFixture>(kind, fanout, threads, BIG_ROWS);
                        MtFixture & fixture = **holder;
                        for (auto _ : state)
                        {
                            fixture.runArm(module_arm);
                            benchmark::ClobberMemory();
                        }
                        state.SetBytesProcessed(
                            static_cast<int64_t>(state.iterations()) * fixture.bytesPerWorker() * fixture.threads);
                        state.counters["threads"] = static_cast<double>(fixture.threads);
                    })
                    ->UseRealTime();
            }
        }
    }
}

void registerFallbackBenchmarks()
{
    for (size_t fanout : {64uz, 256uz})
    {
        for (const char * type_name : {"Variant(UInt64, String)", "Dynamic"})
        {
            auto fixture = std::make_shared<FallbackFixture>(fanout, type_name);
            std::string label = std::string("BM_fallback_") + (std::string(type_name).starts_with("Variant") ? "variant" : "dynamic");
            benchmark::RegisterBenchmark(
                (label + "/F" + std::to_string(fanout)).c_str(),
                [fixture](benchmark::State & state)
                {
                    for (auto _ : state)
                    {
                        auto shards = fixture->run();
                        benchmark::DoNotOptimize(shards.data());
                    }
                    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * fixture->n * 8);
                    state.counters["rows"] = static_cast<double>(fixture->n);
                });
        }
    }
}


MutableColumnPtr fillRandomFixed(MutableColumnPtr column, size_t rows)
{
    auto raw = column->insertRawUninitialized(rows);
    pcg64 rng(49);
    for (auto & byte : raw)
        byte = static_cast<char>(rng());
    return column;
}

/// U4: width-matched cells. Same shape as the 8-byte cells: for each width w in the gate-table
/// width list (dispatched narrow widths, 32, and the generic-path widths 7/33), the reference arm
/// runs the U0-frozen verbatim `scatterOne` at width w and the module arm runs `scatterPidChunk`
/// with the same runtime width into the SAME preallocated per-shard buffers — the D-0006 basis
/// for fixed widths in the Gate-1 table.
struct WidthFixture
{
    size_t fanout;
    size_t width;
    size_t n;
    bool use_swwc;
    PaddedPODArray<char> data;
    PaddedPODArray<UInt16> pids;
    std::vector<PaddedPODArray<char>> parts;
    std::vector<char *> bases;
    ScatterScratch ref_scratch;
    ColumnsScatter::ScatterScratch mod_scratch;

    WidthFixture(size_t fanout_, size_t width_)
        : fanout(fanout_)
        , width(width_)
        , n(scatterBatchRowsTarget(fanout_))
        , use_swwc(fanout_ >= SWWC_MIN_FANOUT && widthSupportsSwwc(width_))
    {
        data.resize(n * width);
        pids.resize(n);
        pcg64 rng(45);
        for (auto & byte : data)
            byte = static_cast<char>(rng());
        const UInt32 shift = static_cast<UInt32>(32 - std::countr_zero(fanout));
        const UInt32 mask = static_cast<UInt32>(fanout - 1);
        for (size_t i = 0; i < n; ++i)
            pids[i] = static_cast<UInt16>((routeWord(rng()) >> shift) & mask);

        std::vector<size_t> counts(fanout, 0);
        for (size_t i = 0; i < n; ++i)
            ++counts[pids[i]];
        parts.resize(fanout);
        bases.resize(fanout);
        for (size_t p = 0; p < fanout; ++p)
        {
            parts[p].resize(counts[p] * width);
            bases[p] = parts[p].data();
        }
        ref_scratch.init(fanout, use_swwc);
        mod_scratch.init(fanout, use_swwc);
        verify();
    }

    void runRef()
    {
        for (size_t p = 0; p < fanout; ++p)
            ref_scratch.seed(p, bases[p]);
        referenceScatterByWidth(RouteFromPids{pids.data()}, data.data(), n, width, use_swwc, ref_scratch);
    }

    void runMod()
    {
        for (size_t p = 0; p < fanout; ++p)
            mod_scratch.seed(p, bases[p]);
        ColumnsScatter::scatterPidChunk(width, pids.data(), data.data(), n, use_swwc, mod_scratch);
        mod_scratch.drain();
    }

    void verify()
    {
        /// Destinations are poisoned before each arm so the second check proves genuine writes
        /// (review U4-corr-2: without it a no-op mod arm would pass on the stale ref output).
        auto poison = [&]
        {
            for (auto & part : parts)
                if (!part.empty())
                    std::memset(part.data(), 0xAA, part.size());
        };
        auto check = [&](const char * mode)
        {
            std::vector<size_t> cursor(fanout, 0);
            for (size_t i = 0; i < n; ++i)
            {
                const size_t p = pids[i];
                if (std::memcmp(bases[p] + cursor[p] * width, data.data() + i * width, width) != 0)
                    throw std::runtime_error(std::string("width fixture oracle: bad content in ") + mode);
                ++cursor[p];
            }
        };
        poison();
        runRef();
        check("ref");
        poison();
        runMod();
        check("mod");
    }
};

/// U4: generic Layer-1 full-call cell for composite types (informational per D-0004/D-0005: the
/// one-shot surface owns allocation; the leaf streams are gated by the width-matched cells).
struct Layer1Fixture
{
    size_t fanout;
    size_t n;
    size_t bytes_per_iteration;
    MutableColumnPtr column;
    PaddedPODArray<UInt16> pids;
    std::vector<UInt32> counts;

    Layer1Fixture(size_t fanout_, MutableColumnPtr column_, size_t bytes_per_iteration_)
        : fanout(fanout_), n(column_->size()), bytes_per_iteration(bytes_per_iteration_), column(std::move(column_))
    {
        pids.resize(n);
        pcg64 rng(46);
        const UInt32 shift = static_cast<UInt32>(32 - std::countr_zero(fanout));
        const UInt32 mask = static_cast<UInt32>(fanout - 1);
        for (size_t i = 0; i < n; ++i)
            pids[i] = static_cast<UInt16>((routeWord(rng()) >> shift) & mask);
        counts.assign(fanout, 0);
        for (size_t i = 0; i < n; ++i)
            ++counts[pids[i]];
    }

    MutableColumns run(bool with_counts) const
    {
        const IColumn * source = column.get();
        std::span<const UInt16> pid_span(pids.data(), n);
        return ColumnsScatter::scatter(
            std::span<const IColumn * const>(&source, 1),
            std::span<const std::span<const UInt16>>(&pid_span, 1),
            fanout,
            with_counts ? std::span<const UInt32>(counts.data(), counts.size()) : std::span<const UInt32>{});
    }
};

/// Registers one informational Layer-1 full-call cell. The fixture is built lazily on first use
/// (review U4-simplicity-1), so `make` must be a self-contained factory.
void registerLayer1Cell(const std::string & label, std::function<std::unique_ptr<Layer1Fixture>()> make)
{
    auto holder = std::make_shared<std::unique_ptr<Layer1Fixture>>();
    benchmark::RegisterBenchmark(
        label.c_str(),
        [holder, make](benchmark::State & state)
        {
            if (!*holder)
                *holder = make();
            Layer1Fixture & fixture = **holder;
            for (auto _ : state)
            {
                auto shards = fixture.run(true);
                benchmark::DoNotOptimize(shards.data());
            }
            state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * fixture.bytes_per_iteration);
        });
}

void registerGateTableBenchmarks()
{
    static constexpr size_t fanouts[] = {64, 256, 2048, 8192};

    /// Width-matched reference + module parity cells.
    /// Cell-name caveat (review U4-corr-4, frozen into the formal raws): `BM_ref_pid{N}` is
    /// payload width N bytes with 16-bit pids and pairs with `BM_mod0_pid16_w{N}`; it is UNRELATED
    /// to the U1 cells `BM_mod0_pid{16,32}`, whose suffix is the pid integer width.
    for (size_t fanout : fanouts)
    {
        for (size_t width : {1uz, 2uz, 4uz, 7uz, 16uz, 32uz, 33uz})
        {
            auto holder = std::make_shared<std::unique_ptr<WidthFixture>>();
            auto register_cell = [&](const char * arm, auto run)
            {
                benchmark::RegisterBenchmark(
                    (std::string(arm) + std::to_string(width) + "/F" + std::to_string(fanout)).c_str(),
                    [holder, fanout, width, run](benchmark::State & state)
                    {
                        if (!*holder)
                            *holder = std::make_unique<WidthFixture>(fanout, width);
                        WidthFixture & fixture = **holder;
                        for (auto _ : state)
                        {
                            run(fixture);
                            benchmark::ClobberMemory();
                        }
                        state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * fixture.n * fixture.width);
                        state.counters["rows"] = static_cast<double>(fixture.n);
                        state.counters["swwc"] = fixture.use_swwc ? 1 : 0;
                    });
            };
            register_cell("BM_ref_pid", [](WidthFixture & f) { f.runRef(); });
            register_cell("BM_mod0_pid16_w", [](WidthFixture & f) { f.runMod(); });
        }
    }

    /// Composite Layer-1 cells (informational; genuine bytes moved counted per type).
    static constexpr size_t rows = 256 << 10;
    for (size_t fanout : {64uz, 256uz})
    {
        registerLayer1Cell(
            "BM_mod1_tuple/F" + std::to_string(fanout),
            [fanout]
            {
                MutableColumns elements;
                elements.push_back(fillRandomFixed(ColumnUInt64::create(), rows));
                elements.push_back(fillRandomFixed(ColumnUInt32::create(), rows));
                return std::make_unique<Layer1Fixture>(fanout, ColumnTuple::create(std::move(elements)), rows * 12);
            });
        registerLayer1Cell(
            "BM_mod1_array/F" + std::to_string(fanout),
            [fanout]
            {
                auto offsets = ColumnArray::ColumnOffsets::create();
                pcg64 rng(47);
                size_t total = 0;
                for (size_t i = 0; i < rows; ++i)
                {
                    total += rng() % 8;
                    offsets->insert(total);
                }
                return std::make_unique<Layer1Fixture>(
                    fanout,
                    ColumnArray::create(fillRandomFixed(ColumnUInt64::create(), total), std::move(offsets)),
                    total * 8 + rows * 8);
            });
        registerLayer1Cell(
            "BM_mod1_lc8/F" + std::to_string(fanout),
            [fanout]
            {
                auto lc = DataTypeFactory::instance().get("LowCardinality(String)")->createColumn();
                pcg64 rng(48);
                for (size_t i = 0; i < rows; ++i)
                {
                    std::string value = "v_" + std::to_string(rng() % 64);
                    lc->insertData(value.data(), value.size());
                }
                /// 64-entry dictionary => UInt8 indexes: 1 genuine byte moved per row.
                return std::make_unique<Layer1Fixture>(fanout, std::move(lc), rows * 1);
            });
    }

    /// Entry-cost isolation (review lead U1-perf-3): a zero-row Layer-1 call isolates the per-call
    /// entry work; the with/without-precomputed-counts pair isolates the internal counting pass.
    for (size_t fanout : {64uz, 8192uz})
    {
        auto empty_fixture = std::make_shared<Layer1Fixture>(fanout, ColumnUInt64::create(), 0);
        benchmark::RegisterBenchmark(
            ("BM_entry_zero_rows/F" + std::to_string(fanout)).c_str(),
            [empty_fixture](benchmark::State & state)
            {
                for (auto _ : state)
                {
                    auto shards = empty_fixture->run(false);
                    benchmark::DoNotOptimize(shards.data());
                }
            });
        auto counting_fixture = std::make_shared<Layer1Fixture>(fanout, fillRandomFixed(ColumnUInt64::create(), rows), rows * 8);
        for (bool with_counts : {true, false})
        {
            benchmark::RegisterBenchmark(
                (std::string("BM_mod1_") + (with_counts ? "counts" : "nocounts") + "/F" + std::to_string(fanout)).c_str(),
                [counting_fixture, with_counts](benchmark::State & state)
                {
                    for (auto _ : state)
                    {
                        auto shards = counting_fixture->run(with_counts);
                        benchmark::DoNotOptimize(shards.data());
                    }
                    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * counting_fixture->bytes_per_iteration);
                });
        }
    }
}

void registerNullableBenchmarks(const std::vector<std::shared_ptr<ReferenceFixture>> & ref_fixtures)
{
    for (const auto & ref : ref_fixtures)
    {
        auto fixture = std::make_shared<NullableFixture>(*ref);
        auto ref_fixture = std::make_shared<NullableRefFixture>(*fixture);
        benchmark::RegisterBenchmark(
            ("BM_ref_null_interleaved/F" + std::to_string(ref->fanout)).c_str(),
            [fixture, ref_fixture, ref](benchmark::State & state)
            {
                for (auto _ : state)
                {
                    ref_fixture->run();
                    benchmark::ClobberMemory();
                }
                state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * ref->n * 9);
            });
        benchmark::RegisterBenchmark(
            ("BM_mod0_null8/F" + std::to_string(ref->fanout)).c_str(),
            [fixture, ref](benchmark::State & state)
            {
                for (auto _ : state)
                {
                    fixture->run();
                    benchmark::ClobberMemory();
                }
                state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * ref->n * 9);
                state.counters["rows"] = static_cast<double>(ref->n);
                state.counters["swwc"] = ref->use_swwc ? 1 : 0;
            });
    }
}

}

int main(int argc, char ** argv)
{
    auto reference_fixtures = registerReferenceBenchmarks();
    registerStringBenchmarks();
    registerNullableBenchmarks(reference_fixtures);
    registerFallbackBenchmarks();
    registerGateTableBenchmarks();
    registerThreadSweepBenchmarks();
    registerBigThreadSweepBenchmarks();
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
