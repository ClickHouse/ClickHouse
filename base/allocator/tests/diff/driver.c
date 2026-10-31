/*
 * Differential test driver: a deterministic workload against the `je_*` API that prints every observable result.
 *
 * It is built twice: against the reference jemalloc (contrib/jemalloc) and against the new allocator, and the outputs
 * must be identical. Run it without ASLR and pinned to one CPU, with time-dependent features disabled:
 *
 *   MALLOC_CONF=background_thread:false,dirty_decay_ms:-1,muzzy_decay_ms:-1 setarch -R taskset -c 0 ./driver all
 *
 * Usage: driver <section>... where a section is one of the names in `sections` below, or `all`.
 */

#define _GNU_SOURCE
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <jemalloc/jemalloc.h>

#define CHECK_MALLCTL(expr) \
    do \
    { \
        int check_mallctl_error_ = (expr); \
        if (check_mallctl_error_) \
            printf("  %s -> error %d\n", #expr, check_mallctl_error_); \
    } while (0)

static uint64_t rng_state = 42;

/* A deterministic PRNG for the workload itself (splitmix64). */
static uint64_t next_random(void)
{
    uint64_t z = (rng_state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static size_t random_size(void)
{
    /* Mostly small, sometimes large, rarely huge. */
    uint64_t r = next_random();
    switch (r % 16)
    {
        case 0: return (size_t)(next_random() % (4 << 20)) + 1;
        case 1:
        case 2: return (size_t)(next_random() % (256 << 10)) + 1;
        case 3: return (size_t)(next_random() % 64) * 4096 + (next_random() % 3) * 8;
        default: return (size_t)(next_random() % 4096) + 1;
    }
}

static void print_global_stats(const char * label)
{
    uint64_t epoch = 1;
    size_t size = sizeof(epoch);
    CHECK_MALLCTL(je_mallctl("epoch", &epoch, &size, &epoch, size));

    static const char * const names[]
        = {"stats.allocated",
           "stats.active",
           "stats.metadata",
           "stats.metadata_edata",
           "stats.metadata_rtc",
           "stats.metadata_thp",
           "stats.resident",
           "stats.mapped",
           "stats.retained"};
    printf("stats %s:", label);
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i)
    {
        size_t value = 0;
        size = sizeof(value);
        int error = je_mallctl(names[i], &value, &size, NULL, 0);
        if (error)
            printf(" %s=err%d", names[i] + 6, error);
        else
            printf(" %s=%zu", names[i] + 6, value);
    }
    printf("\n");
}

static void print_arena_stats(unsigned arena, const char * label)
{
    uint64_t epoch = 1;
    size_t size = sizeof(epoch);
    CHECK_MALLCTL(je_mallctl("epoch", &epoch, &size, &epoch, size));

    char name[128];
    static const char * const size_names[]
        = {"pactive",
           "pdirty",
           "pmuzzy",
           "mapped",
           "retained",
           "base",
           "internal",
           "allocated_large",
           "resident",
           "metadata_edata",
           "metadata_rtc",
           "extent_avail",
           "tcache_bytes",
           "tcache_stashed_bytes"};
    printf("arena %u stats %s:", arena, label);
    for (size_t i = 0; i < sizeof(size_names) / sizeof(size_names[0]); ++i)
    {
        size_t value = 0;
        size = sizeof(value);
        snprintf(name, sizeof(name), "stats.arenas.%u.%s", arena, size_names[i]);
        int error = je_mallctl(name, &value, &size, NULL, 0);
        if (error)
            printf(" %s=err%d", size_names[i], error);
        else
            printf(" %s=%zu", size_names[i], value);
    }
    static const char * const u64_names[]
        = {"dirty_npurge",
           "dirty_nmadvise",
           "dirty_purged",
           "muzzy_npurge",
           "muzzy_nmadvise",
           "muzzy_purged",
           "nmalloc_large",
           "ndalloc_large",
           "nrequests_large",
           "nfills_large",
           "nflushes_large"};
    for (size_t i = 0; i < sizeof(u64_names) / sizeof(u64_names[0]); ++i)
    {
        uint64_t value = 0;
        size = sizeof(value);
        snprintf(name, sizeof(name), "stats.arenas.%u.%s", arena, u64_names[i]);
        int error = je_mallctl(name, &value, &size, NULL, 0);
        if (error)
            printf(" %s=err%d", u64_names[i], error);
        else
            printf(" %s=%" PRIu64, u64_names[i], value);
    }
    printf("\n");

    unsigned num_bins = 0;
    size = sizeof(num_bins);
    CHECK_MALLCTL(je_mallctl("arenas.nbins", &num_bins, &size, NULL, 0));
    for (unsigned j = 0; j < num_bins; ++j)
    {
        static const char * const bin_u64[] = {"nmalloc", "ndalloc", "nrequests", "nfills", "nflushes", "nslabs", "nreslabs"};
        static const char * const bin_size_t_names[] = {"curregs", "curslabs", "nonfull_slabs"};
        uint64_t values[7];
        size_t size_values[3];
        for (size_t i = 0; i < 7; ++i)
        {
            size = sizeof(uint64_t);
            snprintf(name, sizeof(name), "stats.arenas.%u.bins.%u.%s", arena, j, bin_u64[i]);
            if (je_mallctl(name, &values[i], &size, NULL, 0))
                values[i] = (uint64_t)-1;
        }
        for (size_t i = 0; i < 3; ++i)
        {
            size = sizeof(size_t);
            snprintf(name, sizeof(name), "stats.arenas.%u.bins.%u.%s", arena, j, bin_size_t_names[i]);
            if (je_mallctl(name, &size_values[i], &size, NULL, 0))
                size_values[i] = (size_t)-1;
        }
        if (values[0] == 0 && values[1] == 0 && values[2] == 0)
            continue;
        printf("  bin %u:", j);
        for (size_t i = 0; i < 7; ++i)
            printf(" %s=%" PRIu64, bin_u64[i], values[i]);
        for (size_t i = 0; i < 3; ++i)
            printf(" %s=%zu", bin_size_t_names[i], size_values[i]);
        printf("\n");
    }
}

/* --- Sections ---------------------------------------------------------------------------------------------------- */

static void section_sizes(void)
{
    printf("== sizes\n");
    for (size_t size = 0; size <= 70000; size += (size < 1024 ? 1 : (size < 16384 ? 7 : 61)))
        printf("nallocx %zu -> %zu\n", size, je_nallocx(size, 0));
    for (int shift = 15; shift < 64; ++shift)
        for (int delta = -2; delta <= 2; ++delta)
        {
            size_t size = ((size_t)1 << shift) + (size_t)(ptrdiff_t)delta;
            printf("nallocx %zu -> %zu\n", size, je_nallocx(size, 0));
        }
    for (int log2_align = 0; log2_align < 24; ++log2_align)
        for (size_t size = 1; size < (1 << 22); size = size * 3 + 1)
            printf("nallocx %zu align %d -> %zu\n", size, log2_align, je_nallocx(size, MALLOCX_LG_ALIGN(log2_align)));
}

static void section_malloc_free(void)
{
    printf("== malloc_free\n");
    enum
    {
        N = 4000
    };
    static void * ptrs[N];
    static size_t sizes[N];
    for (int i = 0; i < N; ++i)
    {
        sizes[i] = random_size();
        ptrs[i] = je_malloc(sizes[i]);
        printf("malloc %zu -> %p usable %zu\n", sizes[i], ptrs[i], je_malloc_usable_size(ptrs[i]));
        if (ptrs[i])
            memset(ptrs[i], (int)i, sizes[i]);
    }
    print_global_stats("after malloc");
    for (int i = 0; i < N; i += 2)
    {
        je_free(ptrs[i]);
        ptrs[i] = NULL;
    }
    print_global_stats("after half free");
    for (int i = 0; i < N; i += 2)
    {
        sizes[i] = random_size();
        ptrs[i] = je_malloc(sizes[i]);
        printf("malloc %zu -> %p usable %zu\n", sizes[i], ptrs[i], je_malloc_usable_size(ptrs[i]));
    }
    for (int i = 0; i < N; ++i)
    {
        if (i % 3 == 0)
            je_sdallocx(ptrs[i], sizes[i], 0);
        else
            je_free(ptrs[i]);
    }
    print_global_stats("after free");
    print_arena_stats(0, "after free");
}

static void section_realloc(void)
{
    printf("== realloc\n");
    void * p = NULL;
    size_t size = 0;
    for (int i = 0; i < 3000; ++i)
    {
        size_t new_size = (next_random() % 4 == 0) ? size / 2 + 1 : size + random_size() / 8 + 1;
        if (new_size > (64 << 20))
            new_size = 1;
        void * q = je_realloc(p, new_size);
        printf("realloc %p %zu -> %zu %p usable %zu\n", p, size, new_size, q, je_malloc_usable_size(q));
        p = q;
        size = new_size;
    }
    je_free(p);

    p = je_malloc(100);
    void * q = je_realloc(p, 0);
    printf("realloc(p, 0) -> %p\n", q);
    if (q)
        je_free(q);

    /* xallocx / rallocx with flags. */
    p = je_mallocx(10000, 0);
    for (int i = 0; i < 50; ++i)
    {
        size_t target = random_size();
        size_t extra = next_random() % 4096;
        size_t result = je_xallocx(p, target, extra, 0);
        printf("xallocx %p %zu %zu -> %zu\n", p, target, extra, result);
        q = je_rallocx(p, random_size(), (next_random() % 2) ? MALLOCX_ZERO : MALLOCX_LG_ALIGN(next_random() % 13));
        printf("rallocx -> %p sallocx %zu\n", q, q ? je_sallocx(q, 0) : 0);
        if (q)
            p = q;
    }
    je_dallocx(p, 0);
    print_global_stats("after realloc");
}

static void section_aligned(void)
{
    printf("== aligned\n");
    for (int i = 0; i < 1000; ++i)
    {
        size_t alignment = (size_t)1 << (next_random() % 22);
        size_t size = random_size();
        void * p = NULL;
        int error = je_posix_memalign(&p, alignment, size);
        printf("posix_memalign %zu %zu -> %d %p usable %zu\n", alignment, size, error, p, p ? je_malloc_usable_size(p) : 0);
        void * q = je_aligned_alloc(alignment, size);
        printf("aligned_alloc %zu %zu -> %p\n", alignment, size, q);
        void * r = je_calloc(1 + next_random() % 100, size / 64 + 1);
        printf("calloc -> %p usable %zu\n", r, je_malloc_usable_size(r));
        if (i % 2)
        {
            je_free(p);
            je_free(q);
        }
        je_free(r);
    }
    void * p = (void *)1;
    printf("posix_memalign bad alignment -> %d\n", je_posix_memalign(&p, 3, 10));
    printf("posix_memalign huge -> %d\n", je_posix_memalign(&p, 4096, SIZE_MAX - 10));
    errno = 0;
    void * q = je_malloc(SIZE_MAX - 10);
    printf("malloc huge -> %p errno %d\n", q, errno);
    print_global_stats("after aligned");
}

static void section_mallocx(void)
{
    printf("== mallocx\n");
    unsigned arena = 0;
    size_t size_value = sizeof(arena);
    CHECK_MALLCTL(je_mallctl("arenas.create", &arena, &size_value, NULL, 0));
    printf("arenas.create -> %u\n", arena);

    unsigned thread_cache = 0;
    size_value = sizeof(thread_cache);
    CHECK_MALLCTL(je_mallctl("tcache.create", &thread_cache, &size_value, NULL, 0));
    printf("tcache.create -> %u\n", thread_cache);

    enum
    {
        N = 2000
    };
    static void * ptrs[N];
    static int flags[N];
    for (int i = 0; i < N; ++i)
    {
        int f = 0;
        switch (next_random() % 6)
        {
            case 0: f = MALLOCX_ARENA(arena); break;
            case 1: f = MALLOCX_ARENA(arena) | MALLOCX_TCACHE_NONE; break;
            case 2: f = MALLOCX_TCACHE(thread_cache); break;
            case 3: f = MALLOCX_ZERO | MALLOCX_TCACHE_NONE; break;
            case 4: f = MALLOCX_LG_ALIGN(next_random() % 16); break;
            default: break;
        }
        flags[i] = f;
        size_t size = random_size();
        ptrs[i] = je_mallocx(size, f);
        printf("mallocx %zu %x -> %p sallocx %zu\n", size, (unsigned)f, ptrs[i], je_sallocx(ptrs[i], 0));
    }
    print_arena_stats(arena, "manual arena");
    for (int i = 0; i < N; ++i)
        je_dallocx(ptrs[i], flags[i] & ~(MALLOCX_ZERO | 0x3f));

    CHECK_MALLCTL(je_mallctl("tcache.flush", NULL, NULL, &thread_cache, sizeof(thread_cache)));
    CHECK_MALLCTL(je_mallctl("tcache.destroy", NULL, NULL, &thread_cache, sizeof(thread_cache)));
    CHECK_MALLCTL(je_mallctl("thread.tcache.flush", NULL, NULL, NULL, 0));
    print_arena_stats(arena, "after flush");

    char name[64];
    snprintf(name, sizeof(name), "arena.%u.purge", arena);
    CHECK_MALLCTL(je_mallctl(name, NULL, NULL, NULL, 0));
    print_arena_stats(arena, "after purge");
    snprintf(name, sizeof(name), "arena.%u.reset", arena);
    CHECK_MALLCTL(je_mallctl(name, NULL, NULL, NULL, 0));
    print_arena_stats(arena, "after reset");
    snprintf(name, sizeof(name), "arena.%u.destroy", arena);
    CHECK_MALLCTL(je_mallctl(name, NULL, NULL, NULL, 0));
    print_arena_stats(MALLCTL_ARENAS_DESTROYED, "destroyed");
    print_global_stats("after mallocx");
}

static void section_purge(void)
{
    printf("== purge\n");
    enum
    {
        N = 500
    };
    static void * ptrs[N];
    for (int i = 0; i < N; ++i)
        ptrs[i] = je_malloc((size_t)(next_random() % (1 << 20)) + 1);
    for (int i = 0; i < N; ++i)
        je_free(ptrs[i]);
    print_arena_stats(0, "before purge");
    CHECK_MALLCTL(je_mallctl(
        "arena."
        "4096"
        ".purge",
        NULL,
        NULL,
        NULL,
        0));
    print_arena_stats(0, "after purge");
    print_global_stats("after purge");

    ssize_t decay = 0;
    size_t size = sizeof(decay);
    CHECK_MALLCTL(je_mallctl("arena.0.dirty_decay_ms", &decay, &size, NULL, 0));
    printf("arena.0.dirty_decay_ms = %zd\n", decay);
    decay = 0;
    CHECK_MALLCTL(je_mallctl("arena.0.dirty_decay_ms", NULL, NULL, &decay, sizeof(decay)));
    for (int i = 0; i < N; ++i)
        ptrs[i] = je_malloc((size_t)(next_random() % (1 << 20)) + 1);
    for (int i = 0; i < N; ++i)
        je_free(ptrs[i]);
    print_arena_stats(0, "decay 0");
}

static void print_stats_callback(void * opaque, const char * s)
{
    (void)opaque;
    fputs(s, stdout);
}

static void section_stats_print(void)
{
    printf("== stats_print\n");
    je_malloc_stats_print(print_stats_callback, NULL, "J");
    printf("\n");
    je_malloc_stats_print(print_stats_callback, NULL, "gblxe");
    printf("\n");
}

static void section_mallctl(void)
{
    printf("== ctl\n");
    static const char * const bool_names[]
        = {"config.debug",
           "config.fill",
           "config.prof",
           "config.stats",
           "opt.abort",
           "opt.retain",
           "opt.background_thread",
           "opt.prof",
           "opt.prof_active",
           "opt.tcache",
           "opt.cache_oblivious",
           "background_thread",
           "prof.active",
           "thread.tcache.enabled",
           "thread.prof.active",
           "prof.thread_active_init"};
    for (size_t i = 0; i < sizeof(bool_names) / sizeof(bool_names[0]); ++i)
    {
        bool value = false;
        size_t size_value = sizeof(value);
        int error = je_mallctl(bool_names[i], &value, &size_value, NULL, 0);
        printf("%s = %d (err %d)\n", bool_names[i], (int)value, error);
    }
    static const char * const str_names[]
        = {"version",
           "config.malloc_conf",
           "opt.dss",
           "opt.percpu_arena",
           "opt.metadata_thp",
           "opt.thp",
           "opt.zero_realloc",
           "opt.prof_prefix"};
    for (size_t i = 0; i < sizeof(str_names) / sizeof(str_names[0]); ++i)
    {
        const char * value = NULL;
        size_t size_value = sizeof(value);
        int error = je_mallctl(str_names[i], &value, &size_value, NULL, 0);
        printf("%s = %s (err %d)\n", str_names[i], value ? value : "(null)", error);
    }
    static const char * const unsigned_names[] = {"opt.narenas", "arenas.narenas", "arenas.nbins", "arenas.nhbins", "arenas.nlextents"};
    for (size_t i = 0; i < sizeof(unsigned_names) / sizeof(unsigned_names[0]); ++i)
    {
        unsigned value = 0;
        size_t size_value = sizeof(value);
        int error = je_mallctl(unsigned_names[i], &value, &size_value, NULL, 0);
        printf("%s = %u (err %d)\n", unsigned_names[i], value, error);
    }
    static const char * const size_names[]
        = {"opt.oversize_threshold",
           "opt.lg_extent_max_active_fit",
           "opt.tcache_max",
           "opt.lg_prof_sample",
           "arenas.page",
           "arenas.quantum",
           "arenas.tcache_max",
           "prof.lg_sample",
           "max_background_threads",
           "opt.max_background_threads"};
    for (size_t i = 0; i < sizeof(size_names) / sizeof(size_names[0]); ++i)
    {
        size_t value = 0;
        size_t size_value = sizeof(value);
        int error = je_mallctl(size_names[i], &value, &size_value, NULL, 0);
        printf("%s = %zu (err %d)\n", size_names[i], value, error);
    }
    static const char * const ssize_names[]
        = {"opt.dirty_decay_ms", "opt.muzzy_decay_ms", "arenas.dirty_decay_ms", "arenas.muzzy_decay_ms", "opt.lg_prof_interval"};
    for (size_t i = 0; i < sizeof(ssize_names) / sizeof(ssize_names[0]); ++i)
    {
        ssize_t value = 0;
        size_t size_value = sizeof(value);
        int error = je_mallctl(ssize_names[i], &value, &size_value, NULL, 0);
        printf("%s = %zd (err %d)\n", ssize_names[i], value, error);
    }

    /* Error codes. */
    size_t value = 0;
    size_t size_value = sizeof(value);
    printf("unknown name -> %d\n", je_mallctl("no.such.name", &value, &size_value, NULL, 0));
    printf(
        "stats.background_thread.run_intervals -> %d\n", je_mallctl("stats.background_thread.run_intervals", &value, &size_value, NULL, 0));
    uint32_t small = 0;
    size_value = sizeof(small);
    printf("wrong size read -> %d sz %zu\n", je_mallctl("arenas.page", &small, &size_value, NULL, 0), size_value);
    uint64_t wide = 0;
    size_value = sizeof(wide);
    printf(
        "arenas.nbins into u64 -> %d value %" PRIu64 " sz %zu\n",
        je_mallctl("arenas.nbins", &wide, &size_value, NULL, 0),
        wide,
        size_value);
    printf("write to read-only -> %d\n", je_mallctl("arenas.page", NULL, NULL, &value, sizeof(value)));

    size_t numeric_path[8];
    size_t numeric_path_length = 8;
    printf("nametomib stats.arenas.0.pdirty -> %d len %zu\n", je_mallctl_name_to_numeric_path("stats.arenas.0.pdirty", numeric_path, &numeric_path_length), numeric_path_length);
    for (size_t i = 0; i < numeric_path_length; ++i)
        printf("  numeric_path[%zu] = %zu\n", i, numeric_path[i]);
    numeric_path_length = 8;
    printf("nametomib partial stats.arenas -> %d len %zu\n", je_mallctl_name_to_numeric_path("stats.arenas", numeric_path, &numeric_path_length), numeric_path_length);

    unsigned old_arena = 0;
    size_value = sizeof(old_arena);
    CHECK_MALLCTL(je_mallctl("thread.arena", &old_arena, &size_value, NULL, 0));
    printf("thread.arena = %u\n", old_arena);
}

static void section_profiling(void)
{
    printf("== prof\n");
    bool active = true;
    CHECK_MALLCTL(je_mallctl("thread.prof.active", NULL, NULL, &active, sizeof(active)));
    size_t log2_sample = 10;
    CHECK_MALLCTL(je_mallctl("prof.reset", NULL, NULL, &log2_sample, sizeof(log2_sample)));
    enum
    {
        N = 3000
    };
    static void * ptrs[N];
    for (int i = 0; i < N; ++i)
    {
        size_t size = random_size();
        ptrs[i] = je_malloc(size);
        printf("malloc %zu -> %p usable %zu\n", size, ptrs[i], je_malloc_usable_size(ptrs[i]));
    }
    for (int i = 0; i < N; i += 2)
        je_free(ptrs[i]);
    const char * filename = "driver_prof.heap";
    CHECK_MALLCTL(je_mallctl("prof.dump", NULL, NULL, &filename, sizeof(filename)));
    for (int i = 1; i < N; i += 2)
        je_free(ptrs[i]);
    print_global_stats("after prof");
}

struct batch_alloc_packet
{
    void ** ptrs;
    size_t num;
    size_t size;
    int flags;
};

struct activity_record
{
    unsigned calls;
    uint64_t allocated;
    uint64_t deallocated;
};

static void activity_callback(void * user_context, uint64_t allocated, uint64_t deallocated)
{
    struct activity_record * record = user_context;
    ++record->calls;
    record->allocated = allocated;
    record->deallocated = deallocated;
}

struct activity_thunk
{
    void (*callback)(void *, uint64_t, uint64_t);
    void * uctx;
};

static void safety_check_abort_hook(const char * message)
{
    (void)message;
}

/* The `experimental.*` leaves: batch_alloc, utilization queries, pactivep, arenas_create_ext, activity callback,
 * safety_check_abort. */
static void section_experimental(void)
{
    printf("== experimental\n");

    struct activity_record record = {0, 0, 0};
    struct activity_thunk thunk = {activity_callback, &record};
    struct activity_thunk old_thunk = {NULL, NULL};
    size_t size_value = sizeof(old_thunk);
    printf(
        "activity_callback -> %d\n", je_mallctl("experimental.thread.activity_callback", &old_thunk, &size_value, &thunk, sizeof(thunk)));
    printf("activity_callback old %d\n", old_thunk.callback != NULL);

    unsigned arena = 0;
    size_value = sizeof(arena);
    CHECK_MALLCTL(je_mallctl("experimental.arenas_create_ext", &arena, &size_value, NULL, 0));
    printf("arenas_create_ext -> %u\n", arena);
    extent_hooks_t * default_hooks = NULL;
    size_value = sizeof(default_hooks);
    CHECK_MALLCTL(je_mallctl("arena.0.extent_hooks", &default_hooks, &size_value, NULL, 0));
    struct
    {
        extent_hooks_t * extent_hooks;
        bool metadata_use_hooks;
    } config = {default_hooks, false};
    unsigned arena2 = 0;
    size_value = sizeof(arena2);
    CHECK_MALLCTL(je_mallctl("experimental.arenas_create_ext", &arena2, &size_value, &config, sizeof(config)));
    printf("arenas_create_ext (no metadata hooks) -> %u\n", arena2);
    size_value = sizeof(size_t);
    printf(
        "arenas_create_ext wrong size -> %d sz %zu\n",
        je_mallctl("experimental.arenas_create_ext", &arena2, &size_value, NULL, 0),
        size_value);

    enum
    {
        N = 70000
    };
    static void * ptrs[N];
    static const size_t sizes[] = {8, 11, 100, 4096, 16384, 20000, 100000};
    static const int flag_kinds = 5;
    for (size_t s = 0; s < sizeof(sizes) / sizeof(sizes[0]); ++s)
    {
        for (int k = 0; k < flag_kinds; ++k)
        {
            int f = 0;
            switch (k)
            {
                case 1: f = MALLOCX_ZERO; break;
                case 2: f = MALLOCX_ARENA(arena); break;
                case 3: f = MALLOCX_ARENA(arena2) | MALLOCX_TCACHE_NONE; break;
                case 4: f = MALLOCX_LG_ALIGN(6); break;
                default: break;
            }
            size_t num = (size_t)(next_random() % (sizes[s] <= 100 ? N : 300));
            struct batch_alloc_packet packet = {ptrs, num, sizes[s], f};
            size_t filled = 0;
            size_value = sizeof(filled);
            int error = je_mallctl("experimental.batch_alloc", &filled, &size_value, &packet, sizeof(packet));
            uint64_t hash = 0;
            for (size_t i = 0; i < filled; ++i)
                hash = hash * 1000003 + (uintptr_t)ptrs[i];
            printf(
                "batch_alloc %zu x %zu flags %x -> %d filled %zu first %p last %p hash %" PRIx64 "\n",
                num,
                sizes[s],
                (unsigned)f,
                error,
                filled,
                filled ? ptrs[0] : NULL,
                filled ? ptrs[filled - 1] : NULL,
                hash);

            /* Utilization of a few of the pointers. */
            for (size_t i = 0; i < filled && i < 4; ++i)
            {
                void * in = ptrs[i * (filled / 4)];
                struct
                {
                    void * slabcur_addr;
                    size_t counts[5];
                } out;
                size_value = sizeof(out);
                CHECK_MALLCTL(je_mallctl("experimental.utilization.query", &out, &size_value, &in, sizeof(in)));
                printf(
                    "  query %p -> slabcur %p nfree %zu nregs %zu size %zu bin_nfree %zu bin_nregs %zu\n",
                    in,
                    out.slabcur_addr,
                    out.counts[0],
                    out.counts[1],
                    out.counts[2],
                    out.counts[3],
                    out.counts[4]);
            }
            if (filled >= 2)
            {
                void * in[2] = {ptrs[0], ptrs[filled - 1]};
                size_t out[6];
                size_value = sizeof(out);
                CHECK_MALLCTL(je_mallctl("experimental.utilization.batch_query", out, &size_value, in, sizeof(in)));
                printf("  batch_query -> %zu %zu %zu %zu %zu %zu\n", out[0], out[1], out[2], out[3], out[4], out[5]);
            }
            for (size_t i = 0; i < filled; i += 2)
                je_sdallocx(ptrs[i], sizes[s], f & ~MALLOCX_ZERO);
            for (size_t i = 1; i < filled; i += 2)
                je_sdallocx(ptrs[i], sizes[s], f & ~MALLOCX_ZERO);
        }
    }

    uint64_t epoch = 1;
    size_value = sizeof(epoch);
    CHECK_MALLCTL(je_mallctl("epoch", &epoch, &size_value, &epoch, size_value));
    const unsigned active_pages_arenas[] = {0, arena, arena2};
    for (size_t arena_position = 0; arena_position < 3; ++arena_position)
    {
        unsigned a = active_pages_arenas[arena_position];
        char name[64];
        snprintf(name, sizeof(name), "experimental.arenas.%u.pactivep", a);
        size_t * active_pages_ptr = NULL;
        size_value = sizeof(active_pages_ptr);
        int error = je_mallctl(name, &active_pages_ptr, &size_value, NULL, 0);
        size_t active_pages = 0;
        snprintf(name, sizeof(name), "stats.arenas.%u.pactive", a);
        size_value = sizeof(active_pages);
        int error2 = je_mallctl(name, &active_pages, &size_value, NULL, 0);
        printf(
            "arena %u pactivep -> %d value %zu stats.pactive -> %d %zu\n",
            a,
            error,
            active_pages_ptr ? *active_pages_ptr : 0,
            error2,
            active_pages);
    }

    struct activity_thunk null_thunk = {NULL, NULL};
    size_value = sizeof(old_thunk);
    printf(
        "activity_callback reset -> %d\n",
        je_mallctl("experimental.thread.activity_callback", &old_thunk, &size_value, &null_thunk, sizeof(null_thunk)));
    printf(
        "activity_callback calls %u allocated %" PRIu64 " deallocated %" PRIu64 " same %d\n",
        record.calls,
        record.allocated,
        record.deallocated,
        old_thunk.callback == activity_callback && old_thunk.uctx == &record);

    void (*hook)(const char *) = safety_check_abort_hook;
    void (*old_hook)(const char *) = NULL;
    size_value = sizeof(old_hook);
    printf("safety_check_abort read -> %d\n", je_mallctl("experimental.hooks.safety_check_abort", &old_hook, &size_value, NULL, 0));
    printf("safety_check_abort wrong size -> %d\n", je_mallctl("experimental.hooks.safety_check_abort", NULL, NULL, &hook, 1));
    printf("safety_check_abort set -> %d\n", je_mallctl("experimental.hooks.safety_check_abort", NULL, NULL, &hook, sizeof(hook)));
    hook = NULL;
    printf("safety_check_abort reset -> %d\n", je_mallctl("experimental.hooks.safety_check_abort", NULL, NULL, &hook, sizeof(hook)));
    print_arena_stats(arena, "experimental arena");
    print_global_stats("after experimental");
}

static void section_many_arenas(void)
{
    printf("== many_arenas\n");
    for (unsigned a = 0; a < 8; ++a)
    {
        unsigned arena = a;
        size_t size = sizeof(arena);
        int error = je_mallctl("thread.arena", NULL, NULL, &arena, sizeof(arena));
        printf("thread.arena = %u -> %d\n", a, error);
        for (int i = 0; i < 100; ++i)
        {
            void * p = je_malloc(random_size());
            printf("  %p\n", p);
            if (i % 3)
                je_free(p);
        }
        (void)size;
    }
    print_global_stats("after many arenas");
}


static void pin_to_cpu(int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0)
        printf("sched_setaffinity(%d) failed\n", cpu);
}

static int allowed_cpus(int * cpus, int max)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    sched_getaffinity(0, sizeof(set), &set);
    int n = 0;
    for (int cpu = 0; cpu < CPU_SETSIZE && n < max; ++cpu)
        if (CPU_ISSET(cpu, &set))
            cpus[n++] = cpu;
    return n;
}

static void print_thread_arena(const char * label)
{
    unsigned arena = 0;
    size_t size = sizeof(arena);
    int error = je_mallctl("thread.arena", &arena, &size, NULL, 0);
    printf("%s: thread.arena = %u (err %d)\n", label, arena, error);
}

/* Moves the thread between the allowed CPUs, so that per-CPU arena selection (and migration) is exercised. */
static void section_per_cpu(void)
{
    printf("== percpu\n");
    int cpus[64];
    int num_cpus = allowed_cpus(cpus, 64);
    printf("allowed cpus: %d\n", num_cpus);
    enum
    {
        N = 600
    };
    static void * ptrs[N];
    for (int round = 0; round < 3; ++round)
        for (int c = 0; c < num_cpus; ++c)
        {
            pin_to_cpu(cpus[c]);
            char label[64];
            snprintf(label, sizeof(label), "round %d cpu %d", round, cpus[c]);
            for (int i = 0; i < N; ++i)
            {
                size_t size = random_size();
                ptrs[i] = je_malloc(size);
                printf("malloc %zu -> %p\n", size, ptrs[i]);
            }
            print_thread_arena(label);
            /* Free half here; the other half after migrating to the next CPU (remote frees). */
            for (int i = 0; i < N; i += 2)
                je_free(ptrs[i]);
            pin_to_cpu(cpus[(c + 1) % num_cpus]);
            for (int i = 1; i < N; i += 2)
                je_free(ptrs[i]);
        }
    /* thread.arena with a per-CPU arena index resumes per-CPU selection (ClickHouse fork patch). */
    unsigned arena = 0;
    printf("thread.arena write 0 -> %d\n", je_mallctl("thread.arena", NULL, NULL, &arena, sizeof(arena)));
    print_thread_arena("after write");
    pin_to_cpu(cpus[num_cpus - 1]);
    void * p = je_malloc(100);
    print_thread_arena("after migrate");
    je_free(p);
    pin_to_cpu(cpus[0]);
    for (unsigned a = 0; a < 6; ++a)
        print_arena_stats(a, "percpu");
}

struct thread_args
{
    int cpu;
    int index;
    void ** ptrs;
    int n;
};

static void * thread_alloc_function(void * arg)
{
    struct thread_args * args = arg;
    pin_to_cpu(args->cpu);
    for (int i = 0; i < args->n; ++i)
    {
        size_t size = random_size();
        args->ptrs[i] = je_malloc(size);
        printf("thread %d malloc %zu -> %p\n", args->index, size, args->ptrs[i]);
    }
    /* Free a third locally; the rest is freed by the next thread. */
    for (int i = 0; i < args->n; i += 3)
    {
        je_free(args->ptrs[i]);
        args->ptrs[i] = NULL;
    }
    print_thread_arena("thread");
    return NULL;
}

static void * thread_free_function(void * arg)
{
    struct thread_args * args = arg;
    pin_to_cpu(args->cpu);
    for (int i = 0; i < args->n; ++i)
        if (args->ptrs[i])
            je_free(args->ptrs[i]);
    return NULL;
}

/* Threads run strictly one after another (create, join), so the result is deterministic. Exercises thread-exit
 * cleanup (tcache flush), remote frees and arena binding of new threads. */
static void section_threads(void)
{
    printf("== threads\n");
    int cpus[64];
    int num_cpus = allowed_cpus(cpus, 64);
    enum
    {
        N = 800
    };
    static void * ptrs[N];
    for (int t = 0; t < 8; ++t)
    {
        struct thread_args args = {cpus[t % num_cpus], t, ptrs, N};
        pthread_t thread;
        pthread_create(&thread, NULL, thread_alloc_function, &args);
        pthread_join(thread, NULL);
        struct thread_args free_args = {cpus[(t + 1) % num_cpus], t, ptrs, N};
        pthread_create(&thread, NULL, thread_free_function, &free_args);
        pthread_join(thread, NULL);
        print_global_stats("after threads");
    }
    pin_to_cpu(cpus[0]);
    print_arena_stats(MALLCTL_ARENAS_ALL, "threads");
}

struct section
{
    const char * name;
    void (*function)(void);
};

static const struct section sections[] = {
    {"sizes", section_sizes},
    {"malloc_free", section_malloc_free},
    {"realloc", section_realloc},
    {"aligned", section_aligned},
    {"mallocx", section_mallocx},
    {"purge", section_purge},
    {"many_arenas", section_many_arenas},
    {"percpu", section_per_cpu},
    {"threads", section_threads},
    {"ctl", section_mallctl},
    {"prof", section_profiling},
    {"experimental", section_experimental},
    {"stats_print", section_stats_print},
};

int main(int argc, char ** argv)
{
    setvbuf(stdout, NULL, _IOFBF, 1 << 20);
    for (int i = 1; i < argc; ++i)
    {
        bool found = false;
        for (size_t s = 0; s < sizeof(sections) / sizeof(sections[0]); ++s)
        {
            if (strcmp(argv[i], "all") == 0 || strcmp(argv[i], sections[s].name) == 0)
            {
                sections[s].function();
                found = true;
            }
        }
        if (!found)
        {
            fprintf(stderr, "Unknown section %s\n", argv[i]);
            return 1;
        }
    }
    print_global_stats("final");
    return 0;
}
