#include "coverage.h"

#pragma clang diagnostic ignored "-Wreserved-identifier"


/// WITH_COVERAGE_DEPTH enables the default implementation of code coverage,
/// that dumps a map to the filesystem.

#if WITH_COVERAGE_DEPTH

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <utility>
#include <vector>


/// Minimal re-declaration of the LLVM profiling data structure.
/// Field layout must match compiler-rt/lib/profile/InstrProfiling.h
/// (generated from InstrProfData.inc with IntPtrT = void*).
///
/// Fields in order:
///   uint64_t  NameRef
///   uint64_t  FuncHash
///   void*     CounterPtr   (relative offset: counter = (char*)&record + (intptr_t)CounterPtr)
///   void*     BitmapPtr    (relative offset, similar)
///   void*     FunctionPointer  (absolute virtual address of the function)
///   void*     Values
///   uint32_t  NumCounters
///   uint16_t  NumValueSites[3]  (IPVK_Last+1 = 3)
///   uint32_t  NumBitmapBytes
struct LLVMProfileData  // NOLINT
{
    const uint64_t NameRef;       // NOLINT
    const uint64_t FuncHash;      // NOLINT
    const void * CounterPtr;      // NOLINT  relative: actual counter = (char*)this + (intptr_t)CounterPtr
    const void * BitmapPtr;       // NOLINT  relative, unused here
    const void * FunctionPointer; // NOLINT  absolute virtual address
    void * Values;                // NOLINT
    const uint32_t NumCounters;      // NOLINT
    const uint16_t NumValueSites[3]; // NOLINT  IPVK_Last+1 = 3 (IndirectCall, MemOpSize, VTableTarget)
    const uint32_t NumBitmapBytes;   // NOLINT  at offset 60 after 2-byte implicit padding
};

static_assert(sizeof(LLVMProfileData) == 64,
    "LLVMProfileData size mismatch - field layout must match compiler-rt __llvm_profile_data");
static_assert(offsetof(LLVMProfileData, CounterPtr)     == 16, "LLVMProfileData::CounterPtr offset mismatch");
static_assert(offsetof(LLVMProfileData, FunctionPointer)== 32, "LLVMProfileData::FunctionPointer offset mismatch");
static_assert(offsetof(LLVMProfileData, NumCounters)    == 48, "LLVMProfileData::NumCounters offset mismatch");

extern "C"
{
void __llvm_profile_dump();  // NOLINT
void __llvm_profile_reset_counters();  // NOLINT
void __llvm_profile_set_filename(const char *);  // NOLINT

const LLVMProfileData * __llvm_profile_begin_data();  // NOLINT
const LLVMProfileData * __llvm_profile_end_data();    // NOLINT
uint64_t * __llvm_profile_begin_counters();            // NOLINT
uint64_t * __llvm_profile_end_counters();              // NOLINT
}


namespace
{
    std::mutex g_coverage_mutex;
    std::string g_current_test_name;
    CoverageFlushCallback g_flush_callback;
}

/// ── XRay call-depth tracking ─────────────────────────────────────────────────
///
/// XRay injects nop sleds at every function entry/exit that can be patched at
/// runtime to call a user-supplied handler.  Unlike -finstrument-functions, XRay
/// provides real runtime text addresses for each function (via
/// __xray_function_address), allowing us to build the text_address →
/// profile_data_index mapping that LLVMProfileData::FunctionPointer (always NULL
/// in PIE builds) cannot provide.
///
/// Flow:
///   setCoverageTest('name')  → __xray_patch() + register handler
///   ... test runs, handler records min call depth per function_id ...
///   setCoverageTest('')       → __xray_unpatch(), flush depths
///
/// After depth collection, __xray_function_address(id) + dladdr() resolves each
/// function_id to a demangled symbol name, whose FNV64 hash is matched against
/// LLVMProfileData::NameRef to write g_func_min_depth[profile_index].

#ifdef CLICKHOUSE_XRAY_INSTRUMENT_COVERAGE

#include <xray/xray_interface.h>
#include <dlfcn.h>
#include <atomic>

namespace
{

/// FNV-64 hash matching LLVM's IndexedInstrProf::ComputeHash / __llvm_profile_str2hash.
static uint64_t fnv64(const char * s) __attribute__((xray_never_instrument));
static uint64_t fnv64(const char * s)
{
    uint64_t h = 0xcbf29ce484222325ULL;
    for (; *s; ++s) { h ^= static_cast<uint8_t>(*s); h *= 0x100000001b3ULL; }
    return h;
}

/// Map from XRay function_id → profile_data_index.
/// Built once when XRay is first activated.
static uint32_t *      g_xray_to_profile = nullptr; /// array indexed by xray func_id
static int32_t         g_xray_max_id = 0;
static std::once_flag  g_xray_map_once;

/// Per-function minimum call depth (indexed by profile_data_index), reset per test.
static std::atomic<uint32_t> * g_xray_min_depth = nullptr;
static uint32_t                g_xray_depth_size = 0;

/// Per-thread call depth, reset when generation changes (same as before).
static std::atomic<uint32_t>  g_xray_generation{0};
thread_local uint32_t         g_tls_xray_generation = UINT32_MAX;
thread_local uint32_t         g_tls_xray_depth = 0;
thread_local uint32_t         g_tls_xray_baseline = 0;

static void buildXRayProfileMap() __attribute__((xray_never_instrument));
static void buildXRayProfileMap()
{
    std::call_once(g_xray_map_once, []
    {
        const int32_t max_id = __xray_max_function_id();
        if (max_id <= 0) return;
        g_xray_max_id = max_id;

        /// Build NameRef → profile_data_index map first.
        const LLVMProfileData * begin = __llvm_profile_begin_data(); // NOLINT
        const LLVMProfileData * end   = __llvm_profile_end_data();   // NOLINT
        const uint32_t n = static_cast<uint32_t>(end - begin);
        std::unordered_map<uint64_t, uint32_t> name_hash_to_idx;
        name_hash_to_idx.reserve(n);
        for (uint32_t i = 0; i < n; ++i)
            name_hash_to_idx.emplace(begin[i].NameRef, i);

        /// Allocate depth array.
        g_xray_depth_size = n;
        g_xray_min_depth = new std::atomic<uint32_t>[n]; // NOLINT
        for (uint32_t i = 0; i < n; ++i)
            g_xray_min_depth[i].store(UINT32_MAX, std::memory_order_relaxed);

        /// For each XRay function_id, resolve to profile_data_index via symbol name hash.
        g_xray_to_profile = new uint32_t[static_cast<size_t>(max_id) + 1]; // NOLINT
        std::fill(g_xray_to_profile, g_xray_to_profile + max_id + 1, UINT32_MAX);

        for (int32_t id = 1; id <= max_id; ++id)
        {
            const void * addr = reinterpret_cast<const void *>(__xray_function_address(id));
            if (!addr) continue;
            Dl_info info;
            if (!dladdr(addr, &info) || !info.dli_sname) continue;
            const uint64_t h = fnv64(info.dli_sname);
            const auto it = name_hash_to_idx.find(h);
            if (it != name_hash_to_idx.end())
                g_xray_to_profile[id] = it->second;
        }
    });
}

static void xrayHandler(int32_t func_id, XRayEntryType type) __attribute__((xray_never_instrument));
static void xrayHandler(int32_t func_id, XRayEntryType type)
{
    if (type == XRayEntryType::ENTRY || type == XRayEntryType::LOG_ARGS_ENTRY)
    {
        const uint32_t cur_gen = g_xray_generation.load(std::memory_order_relaxed);
        if (g_tls_xray_generation != cur_gen) [[unlikely]]
        {
            g_tls_xray_baseline    = g_tls_xray_depth;
            g_tls_xray_generation  = cur_gen;
        }
        const uint32_t abs = ++g_tls_xray_depth;
        const uint32_t rel = (abs > g_tls_xray_baseline) ? abs - g_tls_xray_baseline : 0u;
        const uint32_t depth = (rel < 255u) ? rel : 254u;

        if (g_xray_to_profile && func_id >= 1 && func_id <= g_xray_max_id)
        {
            const uint32_t idx = g_xray_to_profile[func_id];
            if (idx < g_xray_depth_size)
            {
                auto & slot = g_xray_min_depth[idx];
                uint32_t old = slot.load(std::memory_order_relaxed);
                while (old > depth && !slot.compare_exchange_weak(old, depth, std::memory_order_relaxed)) {}
            }
        }
    }
    else if (type == XRayEntryType::EXIT || type == XRayEntryType::TAIL)
    {
        --g_tls_xray_depth;
    }
}

} // anonymous namespace

#endif // CLICKHOUSE_XRAY_INSTRUMENT_COVERAGE

/// Stubs for -finstrument-functions (kept for link compatibility with builds that
/// add the flag without XRay).
extern "C" void __cyg_profile_func_enter(void *, void *) __attribute__((no_instrument_function));
void __cyg_profile_func_enter(void *, void *) {}

extern "C" void __cyg_profile_func_exit(void *, void *) __attribute__((no_instrument_function));
void __cyg_profile_func_exit(void *, void *) {}


std::vector<CovCounter> getCurrentCoveredNameRefs()
{
    const LLVMProfileData * begin = __llvm_profile_begin_data(); // NOLINT
    const LLVMProfileData * end   = __llvm_profile_end_data();   // NOLINT

    const uint64_t * const cnts_begin = __llvm_profile_begin_counters(); // NOLINT
    const uint64_t * const cnts_end   = __llvm_profile_end_counters();   // NOLINT

    const std::size_t total = static_cast<std::size_t>(end - begin);

    std::vector<CovCounter> result;
    /// Reserve assuming ~4 non-zero counters per function on average.
    result.reserve(std::min(total * 4, std::size_t{1 << 20}));

    const uint32_t n_profile = static_cast<uint32_t>(end - begin);

    for (uint32_t idx = 0; idx < n_profile; ++idx)
    {
        const LLVMProfileData * data = begin + idx;

        if (!data->NumCounters)
            continue;

        /// CounterPtr is a relative signed offset from the address of the data record.
        const uint64_t * const entry_counter = reinterpret_cast<const uint64_t *>(
            reinterpret_cast<const char *>(data) + reinterpret_cast<intptr_t>(data->CounterPtr));

        /// Validate the entire counter array fits within the counters section.
        if (entry_counter < cnts_begin || entry_counter + data->NumCounters > cnts_end)
            continue;

        /// Skip functions that were never entered — branch counters inside them
        /// would be false positives.
        if (*entry_counter == 0)
            continue;

        /// min_depth: prefer XRay call depth (exact, built via text_addr→profile mapping)
        /// over the call-count proxy.  XRay depth is populated only when the binary is
        /// built with -DCLICKHOUSE_XRAY_INSTRUMENT_COVERAGE=1 and XRay is activated.
        uint8_t min_depth;
#ifdef CLICKHOUSE_XRAY_INSTRUMENT_COVERAGE
        if (g_xray_min_depth && idx < g_xray_depth_size)
        {
            const uint32_t xray_d = g_xray_min_depth[idx].load(std::memory_order_relaxed);
            min_depth = (xray_d < 255u) ? static_cast<uint8_t>(xray_d) : 255u;
        }
        else
#endif
        {
            /// Fallback: raw entry-counter call count (lower = more specific).
            min_depth = static_cast<uint8_t>(std::min<uint64_t>(*entry_counter, 254u));
        }

        /// Emit one entry per non-zero counter.  Counter 0 is the function entry;
        /// counters 1…N are individual basic-block/branch counters that map to
        /// specific statement-level regions in the LLVM coverage mapping.
        for (uint32_t i = 0; i < data->NumCounters; ++i)
        {
            if (entry_counter[i] > 0)
                result.emplace_back(data->NameRef, data->FuncHash, i, min_depth);
        }
    }

    return result;
}


/// LLVM value-profiling node: records one (value, count) pair for indirect calls.
/// Matches compiler-rt's ValueProfNode layout.
struct ValueProfNode
{
    uint64_t value; /// Absolute runtime address of the callee
    uint64_t count;
    ValueProfNode * next;
};

std::vector<IndirectCallEntry> getCurrentIndirectCalls()
{
    std::vector<IndirectCallEntry> result;

    /// Use the __executable_start linker symbol to get the binary's ELF load address.
    /// This is defined by the default linker script on Linux and equals the virtual
    /// address at which the binary is loaded — identical to what ASLR assigns.
    /// It is always available (no parsing required) and is more reliable than reading
    /// /proc/self/maps, which can fail in containers, be stale, or match an unrelated
    /// r-xp mapping before the main binary's text segment.
    ///
    /// With a correct load_base, callee_offset = callee_address - load_base is a
    /// stable file-relative offset that stays constant across ASLR restarts for the
    /// same binary build, enabling cross-run joins in the coverage database.
    extern char __executable_start; // NOLINT — linker-defined symbol (available in both GNU ld and lld)
    const uintptr_t load_base  = reinterpret_cast<uintptr_t>(&__executable_start);

    const LLVMProfileData * begin = __llvm_profile_begin_data(); // NOLINT
    const LLVMProfileData * end   = __llvm_profile_end_data();   // NOLINT

    for (const LLVMProfileData * data = begin; data != end; ++data)
    {
        /// IPVK_IndirectCallTarget is kind 0; NumValueSites[0] is the number of
        /// indirect-call sites instrumented in this function.
        if (data->NumValueSites[0] == 0 || !data->Values)
            continue;

        /// Values → array of ValueProfNode* heads, one per indirect-call site.
        /// Cast away const: we zero each node's count after reading so the next
        /// test starts fresh.  __llvm_profile_reset_counters() only resets the
        /// regular counter array, NOT the value-profiling linked lists, so without
        /// this manual reset every test would accumulate all prior indirect calls.
        auto * const sites = static_cast<ValueProfNode * const *>(data->Values);

        for (uint16_t s = 0; s < data->NumValueSites[0]; ++s)
        {
            for (ValueProfNode * node = const_cast<ValueProfNode *>(sites[s]); node; node = node->next)
            {
                if (node->count == 0 || node->value == 0)
                    continue;
                const uint64_t call_count = node->count;
                node->count = 0; /// reset for next test — prevents cross-test accumulation
                /// Only record callees that lie above __executable_start (main binary).
                /// Shared-library callees with ASLR addresses below the binary base are
                /// filtered here; remaining ubiquitous callees (glibc, libstdc++) are
                /// filtered at query time via HAVING uniqExact(test_name) < N.
                /// Note: __etext is not exported by lld, so we skip the upper-bound check.
                if (node->value < load_base)
                    continue;
                const uint64_t offset = node->value - load_base;
                result.push_back({data->NameRef, data->FuncHash, offset, call_count});
            }
        }
    }

    return result;
}


void registerCoverageFlushCallback(CoverageFlushCallback cb)
{
    std::lock_guard lock(g_coverage_mutex);
    g_flush_callback = std::move(cb);
}


void setCoverageTest(std::string_view test_name)
{
    /// We collect what we need under the lock, then release it before invoking
    /// the callback (which may do heavy DB work and must not hold the mutex).
    std::string prev_test_name;
    std::vector<CovCounter> name_refs;
    std::vector<IndirectCallEntry> indirect_calls;
    CoverageFlushCallback cb;

    {
        std::lock_guard lock(g_coverage_mutex);

        if (!g_current_test_name.empty() && g_flush_callback)
        {
            /// Collect covered NameRefs and indirect-call observations before reset.
            name_refs      = getCurrentCoveredNameRefs();
            indirect_calls = getCurrentIndirectCalls();
            prev_test_name = g_current_test_name;
            cb = g_flush_callback;
        }

        __llvm_profile_reset_counters(); // NOLINT

#ifdef CLICKHOUSE_XRAY_INSTRUMENT_COVERAGE
        /// Bump XRay generation so every thread resets its depth baseline on next call.
        g_xray_generation.fetch_add(1, std::memory_order_relaxed);
        /// Reset per-function min-depth array.
        for (uint32_t i = 0; i < g_xray_depth_size; ++i)
            g_xray_min_depth[i].store(UINT32_MAX, std::memory_order_relaxed);
        if (!g_current_test_name.empty())
        {
            /// Deactivate XRay after collecting the previous test's data.
            __xray_unpatch();
        }
        if (!test_name.empty())
        {
            /// Build the XRay→profile map lazily on first test start.
            buildXRayProfileMap();
            __xray_set_handler(xrayHandler);
            __xray_patch();
        }
#endif

        g_current_test_name = std::string(test_name);
    }

    /// Invoke the callback outside the lock so the DB layer can look up
    /// source locations and insert into system.coverage_log without risking deadlock.
    if (cb)
        cb(prev_test_name, name_refs, indirect_calls);
}

void resetCoverage()
{
    setCoverageTest("");
}

#else

void setCoverageTest(std::string_view)
{
}

void resetCoverage()
{
}

#endif


void dumpCoverageReportIfPossible()
{
#if WITH_COVERAGE_DEPTH
    static std::mutex mutex;
    std::lock_guard lock(mutex);
    __llvm_profile_dump(); // NOLINT
#endif
}


void loadCoverageMapping()
{
    /// Coverage mapping is loaded lazily on the first call to collectAndInsertCoverage
    /// (in CoverageCollection.cpp). This function is kept as a placeholder for potential
    /// eager initialization in the future.
}
