#include "coverage.h"

#pragma GCC diagnostic ignored "-Wreserved-identifier"


/// WITH_COVERAGE enables the default implementation of code coverage,
/// that dumps a map to the filesystem.

#if WITH_COVERAGE

#include <mutex>
#include <unistd.h>


#    if defined(__clang__)
extern "C" void __llvm_profile_dump(); // NOLINT
#    elif defined(__GNUC__) || defined(__GNUG__)
extern "C" void __gcov_exit();
#    endif

#endif


void dumpCoverageReportIfPossible()
{
#if WITH_COVERAGE
    static std::mutex mutex;
    std::lock_guard lock(mutex);

#    if defined(__clang__)
    __llvm_profile_dump(); // NOLINT
#    elif defined(__GNUC__) || defined(__GNUG__)
    __gcov_exit();
#    endif

#endif
}


/// SANITIZE_COVERAGE enables code instrumentation,
/// but leaves the callbacks implementation to us,
/// which we use to calculate coverage on a per-test basis
/// and to write it to system tables.

#if defined(SANITIZE_COVERAGE)

namespace
{
    bool pc_guards_initialized = false;
    bool pc_table_initialized = false;

    uint32_t * guards_start = nullptr;
    uint32_t * guards_end = nullptr;

    uintptr_t * coverage_array = nullptr;
    size_t coverage_array_size = 0;

    uintptr_t * all_addresses_array = nullptr;
    size_t all_addresses_array_size = 0;
}

extern "C"
{

/// This is called at least once for every DSO for initialization.
/// But we will use it only for the main DSO.
void __sanitizer_cov_trace_pc_guard_init(uint32_t * start, uint32_t * stop)
{
    if (pc_guards_initialized)
        return;
    pc_guards_initialized = true;

    /// The function can be called multiple times, but we need to initialize only once.
    if (start == stop || *start)
        return;

    guards_start = start;
    guards_end = stop;
    coverage_array_size = stop - start;

    /// Note: we will leak this.
    coverage_array = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * coverage_array_size));

    resetCoverage();
}

/// This is called at least once for every DSO for initialization
/// and provides information about all instrumented addresses.
void __sanitizer_cov_pcs_init(const uintptr_t * pcs_begin, const uintptr_t * pcs_end)
{
    if (pc_table_initialized)
        return;
    pc_table_initialized = true;

    all_addresses_array = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * coverage_array_size));
    all_addresses_array_size = pcs_end - pcs_begin;

    /// They are not a real pointers, but also contain a flag in the most significant bit,
    /// in which we are not interested for now. Reset it.
    for (size_t i = 0; i < all_addresses_array_size; ++i)
        all_addresses_array[i] = pcs_begin[i] & 0x7FFFFFFFFFFFFFFFULL;
}

/// This is called at every basic block / edge, etc.
void __sanitizer_cov_trace_pc_guard(uint32_t * guard)
{
    /// Duplicate the guard check.
    if (!*guard)
        return;
    *guard = 0;

    /// If you set *guard to 0 this code will not be called again for this edge.
    /// Now we can get the PC and do whatever you want:
    /// - store it somewhere or symbolize it and print right away.
    /// The values of `*guard` are as you set them in
    /// __sanitizer_cov_trace_pc_guard_init and so you can make them consecutive
    /// and use them to dereference an array or a bit vector.
    void * pc = __builtin_return_address(0);

    coverage_array[guard - guards_start] = reinterpret_cast<uintptr_t>(pc);
}

}

__attribute__((no_sanitize("coverage")))
std::span<const uintptr_t> getCoverage()
{
    return {coverage_array, coverage_array_size};
}

__attribute__((no_sanitize("coverage")))
std::span<const uintptr_t> getAllInstrumentedAddresses()
{
    return {all_addresses_array, all_addresses_array_size};
}

__attribute__((no_sanitize("coverage")))
void resetCoverage()
{
    memset(coverage_array, 0, coverage_array_size * sizeof(*coverage_array));

    /// The guard defines whether the __sanitizer_cov_trace_pc_guard should be called.
    /// For example, you can unset it after first invocation to prevent excessive work.
    /// Initially set all the guards to 1 to enable callbacks.
    for (uint32_t * x = guards_start; x < guards_end; ++x)
        *x = 1;
}

#else

std::span<const uintptr_t> getCoverage()
{
    return {};
}

std::span<const uintptr_t> getAllInstrumentedAddresses()
{
    return {};
}

void resetCoverage()
{
}

#endif
