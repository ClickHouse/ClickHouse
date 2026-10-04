#include <allocator/Options.h>

#include <allocator/ExtentHooks.h>

namespace jemalloc
{

constinit Options opt{};

constinit const char * const zero_realloc_mode_names[3] = {
    "alloc",
    "free",
    "abort",
};

constinit const char * const percpu_arena_mode_names[5] = {"percpu", "phycpu", "disabled", "percpu", "phycpu"};

constinit const char * const hpa_hugify_style_names[4] = {"auto", "none", "eager", "lazy"};

constinit const char * const prof_time_res_mode_names[2] = {
    "default",
    "high",
};

namespace
{

/// Current DSS precedence default, used when creating new arenas. Stored as `unsigned` as in jemalloc.
/// jemalloc: dss_prec_default (`extent_dss.c`)
constinit std::atomic<unsigned> dss_prec_default{unsigned(DSS_PREC_DEFAULT)};

}

/// jemalloc: extent_dss_prec_get
DssPrec extentDssPrecGet()
{
    if constexpr (!config::have_dss)
        return DssPrec::Disabled;
    return DssPrec(dss_prec_default.load(std::memory_order_acquire));
}

/// jemalloc: extent_dss_prec_set
bool extentDssPrecSet(DssPrec dss_prec)
{
    if constexpr (!config::have_dss)
        return dss_prec != DssPrec::Disabled;
    dss_prec_default.store(unsigned(dss_prec), std::memory_order_release);
    return false;
}

}
