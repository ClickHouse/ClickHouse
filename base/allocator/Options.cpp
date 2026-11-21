#include <allocator/Options.h>

#include <allocator/ExtentHooks.h>

namespace jemalloc
{

constinit Options options{};

constinit const char * const zero_realloc_mode_names[3] = {
    "alloc",
    "free",
    "abort",
};

constinit const char * const per_cpu_arena_mode_names[5] = {"percpu", "phycpu", "disabled", "percpu", "phycpu"};

constinit const char * const huge_page_allocator_hugify_style_names[4] = {"auto", "none", "eager", "lazy"};

constinit const char * const profiling_time_resolution_mode_names[2] = {
    "default",
    "high",
};

namespace
{

/// Current DSS precedence default, used when creating new arenas. Stored as `unsigned` as in jemalloc.
/// jemalloc: dss_prec_default (`extent_dss.c`)
constinit std::atomic<unsigned> sbrk_precedence_default{unsigned(SBRK_PRECEDENCE_DEFAULT)};

}

/// jemalloc: extent_dss_prec_get
SbrkPrecedence extentSbrkPrecedenceGet()
{
    if constexpr (!config::have_sbrk)
        return SbrkPrecedence::Disabled;
    return SbrkPrecedence(sbrk_precedence_default.load(std::memory_order_acquire));
}

/// jemalloc: extent_dss_prec_set
bool extentSbrkPrecedenceSet(SbrkPrecedence sbrk_precedence)
{
    if constexpr (!config::have_sbrk)
        return sbrk_precedence != SbrkPrecedence::Disabled;
    sbrk_precedence_default.store(unsigned(sbrk_precedence), std::memory_order_release);
    return false;
}

}
