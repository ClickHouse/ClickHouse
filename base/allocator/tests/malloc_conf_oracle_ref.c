/* The reference for `conf_oracle`: runs jemalloc's own `malloc_conf_init` (with the boot steps that precede it in
 * `malloc_init_hard_a0_locked`) and prints all option values in the format of `dumpOptions` in conf_oracle.cpp. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/bin.h"
#include "jemalloc/internal/conf.h"
#include "jemalloc/internal/extent_dss.h"
#include "jemalloc/internal/extent_mmap.h"
#include "jemalloc/internal/hpa.h"
#include "jemalloc/internal/san.h"
#include "jemalloc/internal/sc.h"

#include <stdio.h>

/* `static` in tcache.c; made global by objcopy (conf_oracle.cmake). */
extern cache_bin_info_t opt_tcache_ncached_max[TCACHE_NBINS_MAX];
extern bool opt_tcache_ncached_max_set[TCACHE_NBINS_MAX];

/* Defined in conf.c (declared only with `JEMALLOC_JET`). */
extern bool had_conf_error;

/* The library is built with `JEMALLOC_PROF_LIBUNWIND`; the profiler's backtrace is never called here. */
int unw_backtrace(void ** buffer, int size)
{
    (void)buffer;
    (void)size;
    return 0;
}

#define B(name, value) dprintf(1, "%s=%s\n", name, (value) ? "true" : "false")
#define U(name, value) dprintf(1, "%s=%llu\n", name, (unsigned long long)(value))
#define I(name, value) dprintf(1, "%s=%lld\n", name, (long long)(value))
#define S(name, value) dprintf(1, "%s=%s\n", name, (value) != NULL ? (value) : "(null)")

static void dump(const sc_data_t * size_class_data, const unsigned * bin_shard_sizes, const char * readlink_buf)
{
    B("abort", opt_abort);
    B("abort_conf", opt_abort_conf);
    B("confirm_conf", opt_confirm_conf);
    S("junk", opt_junk);
    B("junk_alloc", opt_junk_alloc);
    B("junk_free", opt_junk_free);
    B("trust_madvise", opt_trust_madvise);
    B("cache_oblivious", opt_cache_oblivious);
    U("zero_realloc", opt_zero_realloc_action);
    B("disable_large_size_classes", opt_disable_large_size_classes);
    B("utrace", opt_utrace);
    B("xmalloc", opt_xmalloc);
    B("experimental_infallible_new", opt_experimental_infallible_new);
    B("experimental_tcache_gc", opt_experimental_tcache_gc);
    B("zero", opt_zero);
    U("narenas", opt_narenas);
    U("narenas_ratio", opt_narenas_ratio);
    U("debug_double_free_max_scan", opt_debug_double_free_max_scan);
    U("calloc_madvise_threshold", opt_calloc_madvise_threshold);
    B("hpa", opt_hpa);
    U("hpa_slab_max_alloc", opt_hpa_opts.slab_max_alloc);
    U("hpa_hugification_threshold", opt_hpa_opts.hugification_threshold);
    U("hpa_dirty_mult", opt_hpa_opts.dirty_mult);
    B("hpa_deferral_allowed", opt_hpa_opts.deferral_allowed);
    U("hpa_hugify_delay_ms", opt_hpa_opts.hugify_delay_ms);
    B("hpa_hugify_sync", opt_hpa_opts.hugify_sync);
    U("hpa_min_purge_interval_ms", opt_hpa_opts.min_purge_interval_ms);
    I("experimental_hpa_max_purge_nhp", opt_hpa_opts.experimental_max_purge_nhp);
    U("hpa_purge_threshold", opt_hpa_opts.purge_threshold);
    U("hpa_min_purge_delay_ms", opt_hpa_opts.min_purge_delay_ms);
    U("hpa_hugify_style", opt_hpa_opts.hugify_style);
    U("hpa_sec_nshards", opt_hpa_sec_opts.nshards);
    U("hpa_sec_max_alloc", opt_hpa_sec_opts.max_alloc);
    U("hpa_sec_max_bytes", opt_hpa_sec_opts.max_bytes);
    U("hpa_sec_batch_fill_extra", opt_hpa_sec_opts.batch_fill_extra);
    B("experimental_hpa_start_huge_if_thp_always", opt_experimental_hpa_start_huge_if_thp_always);
    B("experimental_hpa_enforce_hugify", opt_experimental_hpa_enforce_hugify);
    U("percpu_arena", opt_percpu_arena);
    I("dirty_decay_ms", opt_dirty_decay_ms);
    I("muzzy_decay_ms", opt_muzzy_decay_ms);
    U("oversize_threshold", opt_oversize_threshold);
    B("huge_arena_pac_thp", opt_huge_arena_pac_thp);
    U("metadata_thp", opt_metadata_thp);
    U("thp", opt_thp);
    B("retain", opt_retain);
    S("dss", opt_dss);
    U("dss_prec_default", extent_dss_prec_get());
    U("lg_extent_max_active_fit", opt_lg_extent_max_active_fit);
    U("process_madvise_max_batch", opt_process_madvise_max_batch);
    I("mutex_max_spin", opt_mutex_max_spin);
    B("stats_print", opt_stats_print);
    S("stats_print_opts", opt_stats_print_opts);
    I("stats_interval", opt_stats_interval);
    S("stats_interval_opts", opt_stats_interval_opts);
    B("tcache", opt_tcache);
    U("tcache_max", opt_tcache_max);
    U("tcache_nslots_small_min", opt_tcache_nslots_small_min);
    U("tcache_nslots_small_max", opt_tcache_nslots_small_max);
    U("tcache_nslots_large", opt_tcache_nslots_large);
    I("lg_tcache_nslots_mul", opt_lg_tcache_nslots_mul);
    U("tcache_gc_incr_bytes", opt_tcache_gc_incr_bytes);
    U("tcache_gc_delay_bytes", opt_tcache_gc_delay_bytes);
    U("lg_tcache_flush_small_div", opt_lg_tcache_flush_small_div);
    U("lg_tcache_flush_large_div", opt_lg_tcache_flush_large_div);
    dprintf(1, "tcache_ncached_max=");
    for (unsigned i = 0; i < TCACHE_NBINS_MAX; i++)
        dprintf(1, "%u%s,", (unsigned)opt_tcache_ncached_max[i].ncached_max, opt_tcache_ncached_max_set[i] ? "*" : "");
    dprintf(1, "\n");
    B("background_thread", opt_background_thread);
    U("max_background_threads", opt_max_background_threads);
    B("prof", opt_prof);
    B("prof_active", opt_prof_active);
    B("prof_thread_active_init", opt_prof_thread_active_init);
    U("prof_bt_max", opt_prof_bt_max);
    U("lg_prof_sample", opt_lg_prof_sample);
    I("lg_prof_interval", opt_lg_prof_interval);
    B("prof_gdump", opt_prof_gdump);
    B("prof_final", opt_prof_final);
    B("prof_leak", opt_prof_leak);
    B("prof_leak_error", opt_prof_leak_error);
    B("prof_accum", opt_prof_accum);
    B("prof_pid_namespace", opt_prof_pid_namespace);
    S("prof_prefix", opt_prof_prefix);
    B("prof_sys_thread_name", opt_prof_sys_thread_name);
    B("prof_unbias", opt_prof_unbias);
    B("prof_log", opt_prof_log);
    I("prof_recent_alloc_max", opt_prof_recent_alloc_max);
    B("prof_stats", opt_prof_stats);
    U("prof_time_res", opt_prof_time_res);
    U("san_guard_large", opt_san_guard_large);
    U("san_guard_small", opt_san_guard_small);
    I("lg_san_uaf_align", opt_lg_san_uaf_align);
    S("malloc_conf_env_var", opt_malloc_conf_env_var);
    S("malloc_conf_symlink", opt_malloc_conf_symlink);
    S("readlink_buf", readlink_buf);
    B("had_conf_error", had_conf_error);
    dprintf(1, "slab_pgs=");
    for (int i = 0; i < size_class_data->nbins; i++)
        dprintf(1, "%d,", size_class_data->sc[i].pgs);
    dprintf(1, "\n");
    dprintf(1, "bin_shards=");
    for (unsigned i = 0; i < SC_NBINS; i++)
        dprintf(1, "%u,", bin_shard_sizes[i]);
    dprintf(1, "\n");
}

void ref_configuration_run(void)
{
    /* malloc_init_hard_a0_locked */
    sc_data_t sc_data = {0};
    sc_boot(&sc_data);
    unsigned bin_shard_sizes[SC_NBINS];
    bin_shard_sizes_boot(bin_shard_sizes);
    prof_boot0();
    char readlink_buf[PATH_MAX + 1];
    readlink_buf[0] = '\0';
    malloc_conf_init(&sc_data, bin_shard_sizes, readlink_buf);

    /* The check after prof_boot1. */
    if (opt_hpa && !hpa_supported())
    {
        malloc_printf("<jemalloc>: HPA not supported in the current configuration; %s.", opt_abort_conf ? "aborting" : "disabling");
        if (opt_abort_conf)
            malloc_abort_invalid_conf();
        else
            opt_hpa = false;
    }

    dump(&sc_data, bin_shard_sizes, readlink_buf);
}
