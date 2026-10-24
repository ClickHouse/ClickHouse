#include <allocator/Stats.h>

#include <allocator/Arena.h>
#include <allocator/Emitter.h>
#include <allocator/FixedPoint.h>
#include <allocator/Frontend.h>
#include <allocator/Mallctl.h>
#include <allocator/Mutex.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <sys/types.h>

/// The statistics printer (jemalloc: `stats_print` and its helpers in `src/stats.c`).
///
/// Like jemalloc, every value is read through the mallctl machinery (`je_mallctl` semantics for `CTL_GET`, numeric path lookups
/// for the loops), in exactly the same order, so that the values and their consistency are identical. The output goes
/// through the `Emitter`, so the sequence of `write_callback` calls is also identical.

namespace jemalloc
{

namespace
{

/// The size of `prof_stats_t` (`profiling.stats.{bins,lextents}.<i>.{live,accumulated}`).
/// jemalloc: prof_stats_t
struct ProfilingStatsValue
{
    uint64_t request_sum;
    uint64_t count;
};

/// jemalloc: PSSET_NPSIZES (`psset.h`)
constexpr unsigned PAGE_SLAB_SET_NUM_PAGE_SIZES = 64;

/// --- The mallctl access used by the printer (jemalloc: `je_mallctl*`, `xmallctl*` from `ctl.h`). -----------------

/// jemalloc: je_mallctl
int statsMallctl(const char * name, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (ALLOCATOR_UNLIKELY(mallocInit()))
        return EAGAIN;
    ThreadState & thread_state = ThreadState::fetch();
    return mallctlByName(thread_state, name, old_value, old_length_ptr, new_value, new_length);
}

/// jemalloc: je_mallctlnametomib
int statsMallctlNameToNumericPath(const char * name, size_t * numeric_path_ptr, size_t * numeric_path_length_ptr)
{
    if (ALLOCATOR_UNLIKELY(mallocInit()))
        return EAGAIN;
    ThreadState & thread_state = ThreadState::fetch();
    return mallctlNameToNumericPath(thread_state, name, numeric_path_ptr, numeric_path_length_ptr);
}

/// jemalloc: je_mallctlbymib
int statsMallctlByNumericPath(const size_t * numeric_path, size_t numeric_path_length, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (ALLOCATOR_UNLIKELY(mallocInit()))
        return EAGAIN;
    ThreadState & thread_state = ThreadState::fetch();
    return mallctlByNumericPath(thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

/// jemalloc: xmallctl
void statsMallctlOrAbort(const char * name, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (statsMallctl(name, old_value, old_length_ptr, new_value, new_length) != 0)
    {
        printMessage("<jemalloc>: Failure in xmallctl(\"%s\", ...)\n", name);
        abort();
    }
}

/// jemalloc: xmallctlnametomib
void statsMallctlNameToNumericPathOrAbort(const char * name, size_t * numeric_path_ptr, size_t * numeric_path_length_ptr)
{
    if (statsMallctlNameToNumericPath(name, numeric_path_ptr, numeric_path_length_ptr) != 0)
    {
        printMessage("<jemalloc>: Failure in xmallctlnametomib(\"%s\", ...)\n", name);
        abort();
    }
}

/// jemalloc: xmallctlbymib
void statsMallctlByNumericPathOrAbort(
    const size_t * numeric_path, size_t numeric_path_length, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (statsMallctlByNumericPath(numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length) != 0)
    {
        writeMessage("<jemalloc>: Failure in xmallctlbymib()\n");
        abort();
    }
}

/// jemalloc: xmallctlmibnametomib
void statsMallctlExtendNumericPathByNameOrAbort(size_t * numeric_path, size_t numeric_path_length, const char * name, size_t * numeric_path_length_ptr)
{
    if (mallctlExtendNumericPathByName(ThreadState::fetch(), numeric_path, numeric_path_length, name, numeric_path_length_ptr) != 0)
    {
        writeMessage("<jemalloc>: Failure in ctl_mibnametomib()\n");
        abort();
    }
}

/// jemalloc: xmallctlbymibname
void statsMallctlByNumericPathAndNameOrAbort(
    size_t * numeric_path,
    size_t numeric_path_length,
    const char * name,
    size_t * numeric_path_length_ptr,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    if (mallctlByNumericPathAndName(ThreadState::fetch(), numeric_path, numeric_path_length, name, numeric_path_length_ptr, old_value, old_length_ptr, new_value, new_length)
        != 0)
    {
        writeMessage("<jemalloc>: Failure in ctl_bymibname()\n");
        abort();
    }
}

/// jemalloc: CTL_GET
template <typename T>
void mallctlGet(const char * name, T * v)
{
    size_t size = sizeof(T);
    statsMallctlOrAbort(name, static_cast<void *>(v), &size, nullptr, 0);
}

/// jemalloc: CTL_LEAF_PREPARE
void mallctlLeafPrepare(size_t * numeric_path, size_t numeric_path_length, const char * name)
{
    ALLOCATOR_ASSERT(numeric_path_length < MALLCTL_MAX_DEPTH);
    size_t numeric_path_length_new = MALLCTL_MAX_DEPTH;
    statsMallctlExtendNumericPathByNameOrAbort(numeric_path, numeric_path_length, name, &numeric_path_length_new);
    ALLOCATOR_ASSERT(numeric_path_length_new > numeric_path_length);
}

/// jemalloc: CTL_LEAF
template <typename T>
void mallctlLeaf(size_t * numeric_path, size_t numeric_path_length, const char * leaf, T * v)
{
    ALLOCATOR_ASSERT(numeric_path_length < MALLCTL_MAX_DEPTH);
    size_t numeric_path_length_new = MALLCTL_MAX_DEPTH;
    size_t size = sizeof(T);
    statsMallctlByNumericPathAndNameOrAbort(numeric_path, numeric_path_length, leaf, &numeric_path_length_new, static_cast<void *>(v), &size, nullptr, 0);
    ALLOCATOR_ASSERT(numeric_path_length_new == numeric_path_length + 1);
}

/// jemalloc: CTL_MIB_GET
template <typename T>
void mallctlGetWithPathComponent(const char * name, size_t i, T * v, size_t idx)
{
    size_t numeric_path[MALLCTL_MAX_DEPTH];
    size_t numeric_path_length = sizeof(numeric_path) / sizeof(size_t);
    size_t size = sizeof(T);
    statsMallctlNameToNumericPathOrAbort(name, numeric_path, &numeric_path_length);
    numeric_path[idx] = i;
    statsMallctlByNumericPathOrAbort(numeric_path, numeric_path_length, static_cast<void *>(v), &size, nullptr, 0);
}

/// jemalloc: CTL_M1_GET
template <typename T>
void mallctlGetWithPathComponent1(const char * name, size_t i, T * v)
{
    mallctlGetWithPathComponent(name, i, v, 1);
}

/// jemalloc: CTL_M2_GET
template <typename T>
void mallctlGetWithPathComponent2(const char * name, size_t i, T * v)
{
    mallctlGetWithPathComponent(name, i, v, 2);
}

/// --- Helpers ----------------------------------------------------------------------------------------------------

/// jemalloc: rate_per_second
uint64_t ratePerSecond(uint64_t value, uint64_t uptime_ns)
{
    uint64_t billion = 1000000000;
    if (uptime_ns == 0 || value == 0)
        return 0;
    if (uptime_ns < billion)
        return value;
    uint64_t uptime_s = uptime_ns / billion;
    return value / uptime_s;
}

/// Calculate x.yyy and output a string (takes a fixed sized char array). Returns true on error.
/// jemalloc: get_rate_str
bool getRateStr(uint64_t dividend, uint64_t divisor, char (&str)[6])
{
    if (divisor == 0 || dividend > divisor)
    {
        /// The rate is not supposed to be greater than 1.
        return true;
    }
    if (dividend > 0)
        ALLOCATOR_ASSERT(UINT64_MAX / dividend >= 1000);

    unsigned n = static_cast<unsigned>((dividend * 1000) / divisor);
    if (n < 10)
        format(str, 6, "0.00%u", n);
    else if (n < 100)
        format(str, 6, "0.0%u", n);
    else if (n < 1000)
        format(str, 6, "0.%u", n);
    else
        format(str, 6, "1");

    return false;
}

/// A table column with its header column (jemalloc: `COL_HDR_DECLARE` / `COL_HDR_INIT` / `COL_HDR`).
struct ColumnHeader
{
    EmitterColumn column;
    EmitterColumn header;

    /// `human == nullptr` means the column name is the header.
    /// jemalloc: COL_HDR_INIT
    void init(
        EmitterRow & row,
        EmitterRow & header_row,
        const char * column_name,
        const char * human,
        EmitterJustify justify,
        int width,
        EmitterType type)
    {
        column.init(row);
        column.justify = justify;
        column.width = width;
        column.type = type;
        header.init(header_row);
        header.justify = justify;
        header.width = width;
        header.type = EmitterType::Title;
        header.str_value = human != nullptr ? human : column_name;
    }
};

/// jemalloc: COL_INIT
void columnInit(EmitterColumn & column, EmitterRow & row, EmitterJustify justify, int width, EmitterType type)
{
    column.init(row);
    column.justify = justify;
    column.width = width;
    column.type = type;
}

constexpr EmitterJustify LEFT = EmitterJustify::Left;
constexpr EmitterJustify RIGHT = EmitterJustify::Right;

/// --- Mutex statistics -------------------------------------------------------------------------------------------

using MutexColumns64 = EmitterColumn[mutex_profiling_num_uint64_t_counters];
using MutexColumns32 = EmitterColumn[mutex_profiling_num_uint32_t_counters];

/// jemalloc: mutex_stats_init_cols
void mutexStatsInitColumns(
    EmitterRow & row, const char * table_name, EmitterColumn * name, MutexColumns64 & column_uint64_t, MutexColumns32 & column_uint32_t)
{
    if (name != nullptr)
    {
        name->init(row);
        name->justify = LEFT;
        name->width = 21;
        name->type = EmitterType::Title;
        name->str_value = table_name;
    }

    constexpr int WIDTH_uint32_t = 12;
    constexpr int WIDTH_uint64_t = 16;
    for (unsigned k = 0; k < mutex_profiling_num_uint64_t_counters; ++k)
    {
        EmitterColumn & column = column_uint64_t[k];
        column.init(row);
        column.justify = RIGHT;
        column.width = mutex_profiling_uint64_counters[k].derived ? 8 : WIDTH_uint64_t;
        column.type = EmitterType::Title;
        column.str_value = mutex_profiling_uint64_counters[k].human;
    }
    for (unsigned k = 0; k < mutex_profiling_num_uint32_t_counters; ++k)
    {
        EmitterColumn & column = column_uint32_t[k];
        column.init(row);
        column.justify = RIGHT;
        column.width = mutex_profiling_uint32_counters[k].derived ? 8 : WIDTH_uint32_t;
        column.type = EmitterType::Title;
        column.str_value = mutex_profiling_uint32_counters[k].human;
    }
    column_uint64_t[mutex_counter_total_wait_time_per_second].width = 10;
}

/// Reads the counters of one mutex; `numeric_path[0 .. numeric_path_length)` is the numeric path of the mutex node.
/// jemalloc: the common part of mutex_stats_read_global, mutex_stats_read_arena, mutex_stats_read_arena_bin
void mutexStatsReadCounters(
    size_t * numeric_path, size_t numeric_path_length, MutexColumns64 & column_uint64_t, MutexColumns32 & column_uint32_t, uint64_t uptime)
{
    for (unsigned k = 0; k < mutex_profiling_num_uint64_t_counters; ++k)
    {
        const MutexProfilingCounterInfo & info = mutex_profiling_uint64_counters[k];
        EmitterColumn & dst = column_uint64_t[k];
        dst.type = EmitterType::Uint64;
        if (!info.derived)
            mallctlLeaf(numeric_path, numeric_path_length, info.name, &dst.uint64_value);
        else
            dst.uint64_value = ratePerSecond(column_uint64_t[info.base_counter].uint64_value, uptime);
    }
    for (unsigned k = 0; k < mutex_profiling_num_uint32_t_counters; ++k)
    {
        const MutexProfilingCounterInfo & info = mutex_profiling_uint32_counters[k];
        EmitterColumn & dst = column_uint32_t[k];
        dst.type = EmitterType::Uint32;
        if (!info.derived)
            mallctlLeaf(numeric_path, numeric_path_length, info.name, &dst.uint32_value);
        else
            dst.uint32_value = static_cast<uint32_t>(ratePerSecond(column_uint32_t[info.base_counter].uint32_value, uptime));
    }
}

/// jemalloc: mutex_stats_read_global, mutex_stats_read_arena (identical)
void mutexStatsReadNamed(
    size_t * numeric_path,
    size_t numeric_path_length,
    const char * name,
    EmitterColumn * column_name,
    MutexColumns64 & column_uint64_t,
    MutexColumns32 & column_uint32_t,
    uint64_t uptime)
{
    mallctlLeafPrepare(numeric_path, numeric_path_length, name);
    size_t numeric_path_length_name = numeric_path_length + 1;

    column_name->str_value = name;

    mutexStatsReadCounters(numeric_path, numeric_path_length_name, column_uint64_t, column_uint32_t, uptime);
}

/// jemalloc: mutex_stats_read_arena_bin
void mutexStatsReadArenaBin(
    size_t * numeric_path, size_t numeric_path_length, MutexColumns64 & column_uint64_t, MutexColumns32 & column_uint32_t, uint64_t uptime)
{
    mallctlLeafPrepare(numeric_path, numeric_path_length, "mutex");
    size_t numeric_path_length_mutex = numeric_path_length + 1;

    mutexStatsReadCounters(numeric_path, numeric_path_length_mutex, column_uint64_t, column_uint32_t, uptime);
}

/// `row` can be null to avoid emitting in table mode.
/// jemalloc: mutex_stats_emit
void mutexStatsEmit(Emitter & emitter, const EmitterRow * row, MutexColumns64 & column_uint64_t, MutexColumns32 & column_uint32_t)
{
    if (row != nullptr)
        emitter.tableRow(*row);

    for (unsigned k = 0; k < mutex_profiling_num_uint64_t_counters; ++k)
        if (!mutex_profiling_uint64_counters[k].derived)
            emitter.jsonKeyValue(mutex_profiling_uint64_counters[k].name, EmitterType::Uint64, &column_uint64_t[k].uint64_value);
    for (unsigned k = 0; k < mutex_profiling_num_uint32_t_counters; ++k)
        if (!mutex_profiling_uint32_counters[k].derived)
            emitter.jsonKeyValue(mutex_profiling_uint32_counters[k].name, EmitterType::Uint32, &column_uint32_t[k].uint32_value);
}

/// --- Per-arena tables -------------------------------------------------------------------------------------------

/// jemalloc: stats_arena_bins_print
ALLOCATOR_COLD void statsArenaBinsPrint(Emitter & emitter, bool mutex, unsigned i, uint64_t uptime)
{
    size_t page;
    bool in_gap;
    bool in_gap_prev;
    unsigned num_bins;
    unsigned j;

    mallctlGet("arenas.page", &page);

    mallctlGet("arenas.nbins", &num_bins);

    EmitterRow header_row;
    header_row.init();

    EmitterRow row;
    row.init();

    bool profiling_stats_on = config::profiling && options.profiling && options.profiling_stats && i == MALLCTL_ARENAS_ALL;

    ColumnHeader size;
    ColumnHeader idx;
    ColumnHeader allocated;
    ColumnHeader num_allocations_column;
    ColumnHeader num_allocations_per_second;
    ColumnHeader num_deallocations_column;
    ColumnHeader num_deallocations_per_second;
    ColumnHeader num_requests_column;
    ColumnHeader num_requests_per_second;
    ColumnHeader profiling_live_requested;
    ColumnHeader profiling_live_count;
    ColumnHeader profiling_accumulated_requested;
    ColumnHeader profiling_accumulated_count;
    ColumnHeader num_shards_column;
    ColumnHeader current_regions_column;
    ColumnHeader current_slabs_column;
    ColumnHeader non_full_slabs_column;
    ColumnHeader regions;
    ColumnHeader pages;
    ColumnHeader justify_spacer;
    ColumnHeader utilization_column;
    ColumnHeader num_fills_column;
    ColumnHeader num_fills_per_second;
    ColumnHeader num_flushes_column;
    ColumnHeader num_flushes_per_second;
    ColumnHeader num_slabs_column;
    ColumnHeader num_slab_changes_column;
    ColumnHeader num_slab_changes_per_second;

    size.init(row, header_row, "size", nullptr, RIGHT, 20, EmitterType::Size);
    idx.init(row, header_row, "ind", nullptr, RIGHT, 4, EmitterType::Unsigned);
    allocated.init(row, header_row, "allocated", nullptr, RIGHT, 14, EmitterType::Size);
    num_allocations_column.init(row, header_row, "nmalloc", nullptr, RIGHT, 14, EmitterType::Uint64);
    num_allocations_per_second.init(row, header_row, "nmalloc_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    num_deallocations_column.init(row, header_row, "ndalloc", nullptr, RIGHT, 14, EmitterType::Uint64);
    num_deallocations_per_second.init(row, header_row, "ndalloc_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    num_requests_column.init(row, header_row, "nrequests", nullptr, RIGHT, 15, EmitterType::Uint64);
    num_requests_per_second.init(row, header_row, "nrequests_ps", "(#/sec)", RIGHT, 10, EmitterType::Uint64);
    if (profiling_stats_on)
    {
        profiling_live_requested.init(row, header_row, "prof_live_requested", nullptr, RIGHT, 21, EmitterType::Uint64);
        profiling_live_count.init(row, header_row, "prof_live_count", nullptr, RIGHT, 17, EmitterType::Uint64);
        profiling_accumulated_requested.init(row, header_row, "prof_accum_requested", nullptr, RIGHT, 21, EmitterType::Uint64);
        profiling_accumulated_count.init(row, header_row, "prof_accum_count", nullptr, RIGHT, 17, EmitterType::Uint64);
    }
    num_shards_column.init(row, header_row, "nshards", nullptr, RIGHT, 9, EmitterType::Unsigned);
    current_regions_column.init(row, header_row, "curregs", nullptr, RIGHT, 13, EmitterType::Size);
    current_slabs_column.init(row, header_row, "curslabs", nullptr, RIGHT, 13, EmitterType::Size);
    non_full_slabs_column.init(row, header_row, "nonfull_slabs", nullptr, RIGHT, 15, EmitterType::Size);
    regions.init(row, header_row, "regs", nullptr, RIGHT, 5, EmitterType::Unsigned);
    pages.init(row, header_row, "pgs", nullptr, RIGHT, 4, EmitterType::Size);
    /// To buffer a right- and left-justified column.
    justify_spacer.init(row, header_row, "justify_spacer", nullptr, RIGHT, 1, EmitterType::Title);
    utilization_column.init(row, header_row, "util", nullptr, RIGHT, 6, EmitterType::Title);
    num_fills_column.init(row, header_row, "nfills", nullptr, RIGHT, 13, EmitterType::Uint64);
    num_fills_per_second.init(row, header_row, "nfills_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    num_flushes_column.init(row, header_row, "nflushes", nullptr, RIGHT, 13, EmitterType::Uint64);
    num_flushes_per_second.init(row, header_row, "nflushes_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    num_slabs_column.init(row, header_row, "nslabs", nullptr, RIGHT, 13, EmitterType::Uint64);
    num_slab_changes_column.init(row, header_row, "nreslabs", nullptr, RIGHT, 13, EmitterType::Uint64);
    num_slab_changes_per_second.init(row, header_row, "nreslabs_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);

    /// Don't want to actually print the name.
    justify_spacer.header.str_value = " ";
    justify_spacer.column.str_value = " ";

    MutexColumns64 column_mutex64;
    MutexColumns32 column_mutex32;

    MutexColumns64 header_mutex64;
    MutexColumns32 header_mutex32;

    if (mutex)
    {
        mutexStatsInitColumns(row, nullptr, nullptr, column_mutex64, column_mutex32);
        mutexStatsInitColumns(header_row, nullptr, nullptr, header_mutex64, header_mutex32);
    }

    /// We print a "bins:" header as part of the table row; we need to adjust the header size column to compensate.
    size.header.width -= 5;
    emitter.tablePrintf("bins:");
    emitter.tableRow(header_row);
    emitter.jsonArrayKeyValueBegin("bins");

    size_t stats_arenas_numeric_path[MALLCTL_MAX_DEPTH];
    mallctlLeafPrepare(stats_arenas_numeric_path, 0, "stats.arenas");
    stats_arenas_numeric_path[2] = i;
    mallctlLeafPrepare(stats_arenas_numeric_path, 3, "bins");

    size_t arenas_bin_numeric_path[MALLCTL_MAX_DEPTH];
    mallctlLeafPrepare(arenas_bin_numeric_path, 0, "arenas.bin");

    size_t profiling_stats_numeric_path[MALLCTL_MAX_DEPTH];
    if (profiling_stats_on)
        mallctlLeafPrepare(profiling_stats_numeric_path, 0, "prof.stats.bins");

    for (j = 0, in_gap = false; j < num_bins; j++)
    {
        uint64_t num_slabs;
        size_t region_size;
        size_t slab_size;
        size_t current_regions;
        size_t current_slabs;
        size_t non_full_slabs;
        uint32_t num_regions;
        uint32_t num_shards;
        uint64_t num_allocations;
        uint64_t num_deallocations;
        uint64_t num_requests;
        uint64_t num_fills;
        uint64_t num_flushes;
        uint64_t num_slab_changes;
        ProfilingStatsValue profiling_live;
        ProfilingStatsValue profiling_accumulated;

        stats_arenas_numeric_path[4] = j;
        arenas_bin_numeric_path[2] = j;

        mallctlLeaf(stats_arenas_numeric_path, 5, "nslabs", &num_slabs);

        if (profiling_stats_on)
        {
            profiling_stats_numeric_path[3] = j;
            mallctlLeaf(profiling_stats_numeric_path, 4, "live", &profiling_live);
            mallctlLeaf(profiling_stats_numeric_path, 4, "accum", &profiling_accumulated);
        }

        in_gap_prev = in_gap;
        if (profiling_stats_on)
            in_gap = (num_slabs == 0 && profiling_accumulated.count == 0);
        else
            in_gap = (num_slabs == 0);

        if (in_gap_prev && !in_gap)
            emitter.tablePrintf("                     ---\n");

        if (in_gap && !emitter.outputsJSON())
            continue;

        mallctlLeaf(arenas_bin_numeric_path, 3, "size", &region_size);
        mallctlLeaf(arenas_bin_numeric_path, 3, "nregs", &num_regions);
        mallctlLeaf(arenas_bin_numeric_path, 3, "slab_size", &slab_size);
        mallctlLeaf(arenas_bin_numeric_path, 3, "nshards", &num_shards);
        mallctlLeaf(stats_arenas_numeric_path, 5, "nmalloc", &num_allocations);
        mallctlLeaf(stats_arenas_numeric_path, 5, "ndalloc", &num_deallocations);
        mallctlLeaf(stats_arenas_numeric_path, 5, "curregs", &current_regions);
        mallctlLeaf(stats_arenas_numeric_path, 5, "nrequests", &num_requests);
        mallctlLeaf(stats_arenas_numeric_path, 5, "nfills", &num_fills);
        mallctlLeaf(stats_arenas_numeric_path, 5, "nflushes", &num_flushes);
        mallctlLeaf(stats_arenas_numeric_path, 5, "nreslabs", &num_slab_changes);
        mallctlLeaf(stats_arenas_numeric_path, 5, "curslabs", &current_slabs);
        mallctlLeaf(stats_arenas_numeric_path, 5, "nonfull_slabs", &non_full_slabs);

        if (mutex)
            mutexStatsReadArenaBin(stats_arenas_numeric_path, 5, column_mutex64, column_mutex32, uptime);

        emitter.jsonObjectBegin();
        emitter.jsonKeyValue("nmalloc", EmitterType::Uint64, &num_allocations);
        emitter.jsonKeyValue("ndalloc", EmitterType::Uint64, &num_deallocations);
        emitter.jsonKeyValue("curregs", EmitterType::Size, &current_regions);
        emitter.jsonKeyValue("nrequests", EmitterType::Uint64, &num_requests);
        if (profiling_stats_on)
        {
            emitter.jsonKeyValue("prof_live_requested", EmitterType::Uint64, &profiling_live.request_sum);
            emitter.jsonKeyValue("prof_live_count", EmitterType::Uint64, &profiling_live.count);
            emitter.jsonKeyValue("prof_accum_requested", EmitterType::Uint64, &profiling_accumulated.request_sum);
            emitter.jsonKeyValue("prof_accum_count", EmitterType::Uint64, &profiling_accumulated.count);
        }
        emitter.jsonKeyValue("nfills", EmitterType::Uint64, &num_fills);
        emitter.jsonKeyValue("nflushes", EmitterType::Uint64, &num_flushes);
        emitter.jsonKeyValue("nreslabs", EmitterType::Uint64, &num_slab_changes);
        emitter.jsonKeyValue("curslabs", EmitterType::Size, &current_slabs);
        emitter.jsonKeyValue("nonfull_slabs", EmitterType::Size, &non_full_slabs);
        if (mutex)
        {
            emitter.jsonObjectKeyValueBegin("mutex");
            mutexStatsEmit(emitter, nullptr, column_mutex64, column_mutex32);
            emitter.jsonObjectEnd();
        }
        emitter.jsonObjectEnd();

        size_t available_regions = num_regions * current_slabs;
        char utilization[6];
        if (getRateStr(static_cast<uint64_t>(current_regions), static_cast<uint64_t>(available_regions), utilization))
        {
            if (available_regions == 0)
            {
                format(utilization, sizeof(utilization), "1");
            }
            else if (current_regions > available_regions)
            {
                /// Race detected: the counters were read in separate mallctl calls and concurrent operations happened
                /// in between. In this case no meaningful utilization can be computed.
                format(utilization, sizeof(utilization), " race");
            }
            else
            {
                ALLOCATOR_NOT_REACHED();
            }
        }

        size.column.size_value = region_size;
        idx.column.unsigned_value = j;
        allocated.column.size_value = current_regions * region_size;
        num_allocations_column.column.uint64_value = num_allocations;
        num_allocations_per_second.column.uint64_value = ratePerSecond(num_allocations, uptime);
        num_deallocations_column.column.uint64_value = num_deallocations;
        num_deallocations_per_second.column.uint64_value = ratePerSecond(num_deallocations, uptime);
        num_requests_column.column.uint64_value = num_requests;
        num_requests_per_second.column.uint64_value = ratePerSecond(num_requests, uptime);
        if (profiling_stats_on)
        {
            profiling_live_requested.column.uint64_value = profiling_live.request_sum;
            profiling_live_count.column.uint64_value = profiling_live.count;
            profiling_accumulated_requested.column.uint64_value = profiling_accumulated.request_sum;
            profiling_accumulated_count.column.uint64_value = profiling_accumulated.count;
        }
        num_shards_column.column.unsigned_value = num_shards;
        current_regions_column.column.size_value = current_regions;
        current_slabs_column.column.size_value = current_slabs;
        non_full_slabs_column.column.size_value = non_full_slabs;
        regions.column.unsigned_value = num_regions;
        pages.column.size_value = slab_size / page;
        utilization_column.column.str_value = utilization;
        num_fills_column.column.uint64_value = num_fills;
        num_fills_per_second.column.uint64_value = ratePerSecond(num_fills, uptime);
        num_flushes_column.column.uint64_value = num_flushes;
        num_flushes_per_second.column.uint64_value = ratePerSecond(num_flushes, uptime);
        num_slabs_column.column.uint64_value = num_slabs;
        num_slab_changes_column.column.uint64_value = num_slab_changes;
        num_slab_changes_per_second.column.uint64_value = ratePerSecond(num_slab_changes, uptime);

        /// Note that mutex columns were initialized above, if mutex == true.

        emitter.tableRow(row);
    }
    emitter.jsonArrayEnd(); /// Close "bins".

    if (in_gap)
        emitter.tablePrintf("                     ---\n");
}

/// jemalloc: stats_arena_lextents_print
ALLOCATOR_COLD void statsArenaLargeExtentsPrint(Emitter & emitter, unsigned i, uint64_t uptime)
{
    unsigned num_bins;
    unsigned num_large_extents;
    unsigned j;
    bool in_gap;
    bool in_gap_prev;

    mallctlGet("arenas.nbins", &num_bins);
    mallctlGet("arenas.nlextents", &num_large_extents);

    EmitterRow header_row;
    header_row.init();
    EmitterRow row;
    row.init();

    bool profiling_stats_on = config::profiling && options.profiling && options.profiling_stats && i == MALLCTL_ARENAS_ALL;

    ColumnHeader size;
    ColumnHeader idx;
    ColumnHeader allocated;
    ColumnHeader num_allocations_column;
    ColumnHeader num_allocations_per_second;
    ColumnHeader num_deallocations_column;
    ColumnHeader num_deallocations_per_second;
    ColumnHeader num_requests_column;
    ColumnHeader num_requests_per_second;
    ColumnHeader profiling_live_requested;
    ColumnHeader profiling_live_count;
    ColumnHeader profiling_accumulated_requested;
    ColumnHeader profiling_accumulated_count;
    ColumnHeader current_large_extents_column;

    size.init(row, header_row, "size", nullptr, RIGHT, 20, EmitterType::Size);
    idx.init(row, header_row, "ind", nullptr, RIGHT, 4, EmitterType::Unsigned);
    allocated.init(row, header_row, "allocated", nullptr, RIGHT, 13, EmitterType::Size);
    num_allocations_column.init(row, header_row, "nmalloc", nullptr, RIGHT, 13, EmitterType::Uint64);
    num_allocations_per_second.init(row, header_row, "nmalloc_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    num_deallocations_column.init(row, header_row, "ndalloc", nullptr, RIGHT, 13, EmitterType::Uint64);
    num_deallocations_per_second.init(row, header_row, "ndalloc_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    num_requests_column.init(row, header_row, "nrequests", nullptr, RIGHT, 13, EmitterType::Uint64);
    num_requests_per_second.init(row, header_row, "nrequests_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    if (profiling_stats_on)
    {
        profiling_live_requested.init(row, header_row, "prof_live_requested", nullptr, RIGHT, 21, EmitterType::Uint64);
        profiling_live_count.init(row, header_row, "prof_live_count", nullptr, RIGHT, 17, EmitterType::Uint64);
        profiling_accumulated_requested.init(row, header_row, "prof_accum_requested", nullptr, RIGHT, 21, EmitterType::Uint64);
        profiling_accumulated_count.init(row, header_row, "prof_accum_count", nullptr, RIGHT, 17, EmitterType::Uint64);
    }
    current_large_extents_column.init(row, header_row, "curlextents", nullptr, RIGHT, 13, EmitterType::Size);

    /// As with bins, we label the large extents table.
    size.header.width -= 6;
    emitter.tablePrintf("large:");
    emitter.tableRow(header_row);
    emitter.jsonArrayKeyValueBegin("lextents");

    size_t stats_arenas_numeric_path[MALLCTL_MAX_DEPTH];
    mallctlLeafPrepare(stats_arenas_numeric_path, 0, "stats.arenas");
    stats_arenas_numeric_path[2] = i;
    mallctlLeafPrepare(stats_arenas_numeric_path, 3, "lextents");

    size_t arenas_large_extent_numeric_path[MALLCTL_MAX_DEPTH];
    mallctlLeafPrepare(arenas_large_extent_numeric_path, 0, "arenas.lextent");

    size_t profiling_stats_numeric_path[MALLCTL_MAX_DEPTH];
    if (profiling_stats_on)
        mallctlLeafPrepare(profiling_stats_numeric_path, 0, "prof.stats.lextents");

    for (j = 0, in_gap = false; j < num_large_extents; j++)
    {
        uint64_t num_allocations;
        uint64_t num_deallocations;
        uint64_t num_requests;
        size_t large_extent_size;
        size_t current_large_extents;
        ProfilingStatsValue profiling_live;
        ProfilingStatsValue profiling_accumulated;

        stats_arenas_numeric_path[4] = j;
        arenas_large_extent_numeric_path[2] = j;

        mallctlLeaf(stats_arenas_numeric_path, 5, "nmalloc", &num_allocations);
        mallctlLeaf(stats_arenas_numeric_path, 5, "ndalloc", &num_deallocations);
        mallctlLeaf(stats_arenas_numeric_path, 5, "nrequests", &num_requests);

        in_gap_prev = in_gap;
        in_gap = (num_requests == 0);

        if (in_gap_prev && !in_gap)
            emitter.tablePrintf("                     ---\n");

        mallctlLeaf(arenas_large_extent_numeric_path, 3, "size", &large_extent_size);
        mallctlLeaf(stats_arenas_numeric_path, 5, "curlextents", &current_large_extents);

        if (profiling_stats_on)
        {
            profiling_stats_numeric_path[3] = j;
            mallctlLeaf(profiling_stats_numeric_path, 4, "live", &profiling_live);
            mallctlLeaf(profiling_stats_numeric_path, 4, "accum", &profiling_accumulated);
        }

        emitter.jsonObjectBegin();
        if (profiling_stats_on)
        {
            emitter.jsonKeyValue("prof_live_requested", EmitterType::Uint64, &profiling_live.request_sum);
            emitter.jsonKeyValue("prof_live_count", EmitterType::Uint64, &profiling_live.count);
            emitter.jsonKeyValue("prof_accum_requested", EmitterType::Uint64, &profiling_accumulated.request_sum);
            emitter.jsonKeyValue("prof_accum_count", EmitterType::Uint64, &profiling_accumulated.count);
        }
        emitter.jsonKeyValue("curlextents", EmitterType::Size, &current_large_extents);
        emitter.jsonObjectEnd();

        size.column.size_value = large_extent_size;
        idx.column.unsigned_value = num_bins + j;
        allocated.column.size_value = current_large_extents * large_extent_size;
        num_allocations_column.column.uint64_value = num_allocations;
        num_allocations_per_second.column.uint64_value = ratePerSecond(num_allocations, uptime);
        num_deallocations_column.column.uint64_value = num_deallocations;
        num_deallocations_per_second.column.uint64_value = ratePerSecond(num_deallocations, uptime);
        num_requests_column.column.uint64_value = num_requests;
        num_requests_per_second.column.uint64_value = ratePerSecond(num_requests, uptime);
        if (profiling_stats_on)
        {
            profiling_live_requested.column.uint64_value = profiling_live.request_sum;
            profiling_live_count.column.uint64_value = profiling_live.count;
            profiling_accumulated_requested.column.uint64_value = profiling_accumulated.request_sum;
            profiling_accumulated_count.column.uint64_value = profiling_accumulated.count;
        }
        current_large_extents_column.column.size_value = current_large_extents;

        if (!in_gap)
            emitter.tableRow(row);
    }
    emitter.jsonArrayEnd(); /// Close "lextents".
    if (in_gap)
        emitter.tablePrintf("                     ---\n");
}

/// jemalloc: stats_arena_extents_print
ALLOCATOR_COLD void statsArenaExtentsPrint(Emitter & emitter, unsigned i)
{
    unsigned j;
    bool in_gap;
    bool in_gap_prev;
    EmitterRow header_row;
    header_row.init();
    EmitterRow row;
    row.init();

    ColumnHeader size;
    ColumnHeader idx;
    ColumnHeader num_dirty_column;
    ColumnHeader dirty_column;
    ColumnHeader num_muzzy_column;
    ColumnHeader muzzy_column;
    ColumnHeader num_retained_column;
    ColumnHeader retained_column;
    ColumnHeader num_total_column;
    ColumnHeader total_column;

    size.init(row, header_row, "size", nullptr, RIGHT, 20, EmitterType::Size);
    idx.init(row, header_row, "ind", nullptr, RIGHT, 4, EmitterType::Unsigned);
    num_dirty_column.init(row, header_row, "ndirty", nullptr, RIGHT, 13, EmitterType::Size);
    dirty_column.init(row, header_row, "dirty", nullptr, RIGHT, 13, EmitterType::Size);
    num_muzzy_column.init(row, header_row, "nmuzzy", nullptr, RIGHT, 13, EmitterType::Size);
    muzzy_column.init(row, header_row, "muzzy", nullptr, RIGHT, 13, EmitterType::Size);
    num_retained_column.init(row, header_row, "nretained", nullptr, RIGHT, 13, EmitterType::Size);
    retained_column.init(row, header_row, "retained", nullptr, RIGHT, 13, EmitterType::Size);
    num_total_column.init(row, header_row, "ntotal", nullptr, RIGHT, 13, EmitterType::Size);
    total_column.init(row, header_row, "total", nullptr, RIGHT, 13, EmitterType::Size);

    /// Label this section.
    size.header.width -= 8;
    emitter.tablePrintf("extents:");
    emitter.tableRow(header_row);
    emitter.jsonArrayKeyValueBegin("extents");

    size_t stats_arenas_numeric_path[MALLCTL_MAX_DEPTH];
    mallctlLeafPrepare(stats_arenas_numeric_path, 0, "stats.arenas");
    stats_arenas_numeric_path[2] = i;
    mallctlLeafPrepare(stats_arenas_numeric_path, 3, "extents");

    in_gap = false;
    for (j = 0; j < SIZE_CLASS_NUM_PAGE_SIZES; j++)
    {
        size_t num_dirty;
        size_t num_muzzy;
        size_t num_retained;
        size_t total;
        size_t dirty_bytes;
        size_t muzzy_bytes;
        size_t retained_bytes;
        size_t total_bytes;
        stats_arenas_numeric_path[4] = j;

        mallctlLeaf(stats_arenas_numeric_path, 5, "ndirty", &num_dirty);
        mallctlLeaf(stats_arenas_numeric_path, 5, "nmuzzy", &num_muzzy);
        mallctlLeaf(stats_arenas_numeric_path, 5, "nretained", &num_retained);
        mallctlLeaf(stats_arenas_numeric_path, 5, "dirty_bytes", &dirty_bytes);
        mallctlLeaf(stats_arenas_numeric_path, 5, "muzzy_bytes", &muzzy_bytes);
        mallctlLeaf(stats_arenas_numeric_path, 5, "retained_bytes", &retained_bytes);

        total = num_dirty + num_muzzy + num_retained;
        total_bytes = dirty_bytes + muzzy_bytes + retained_bytes;

        in_gap_prev = in_gap;
        in_gap = (total == 0);

        if (in_gap_prev && !in_gap)
            emitter.tablePrintf("                     ---\n");

        emitter.jsonObjectBegin();
        emitter.jsonKeyValue("ndirty", EmitterType::Size, &num_dirty);
        emitter.jsonKeyValue("nmuzzy", EmitterType::Size, &num_muzzy);
        emitter.jsonKeyValue("nretained", EmitterType::Size, &num_retained);

        emitter.jsonKeyValue("dirty_bytes", EmitterType::Size, &dirty_bytes);
        emitter.jsonKeyValue("muzzy_bytes", EmitterType::Size, &muzzy_bytes);
        emitter.jsonKeyValue("retained_bytes", EmitterType::Size, &retained_bytes);
        emitter.jsonObjectEnd();

        size.column.size_value = size_classes::pageSizeClassIdxToSize(j);
        /// jemalloc compatibility: the column has the `unsigned` type but the value is assigned via `size_value`.
        idx.column.size_value = j;
        num_dirty_column.column.size_value = num_dirty;
        dirty_column.column.size_value = dirty_bytes;
        num_muzzy_column.column.size_value = num_muzzy;
        muzzy_column.column.size_value = muzzy_bytes;
        num_retained_column.column.size_value = num_retained;
        retained_column.column.size_value = retained_bytes;
        num_total_column.column.size_value = total;
        total_column.column.size_value = total_bytes;

        if (!in_gap)
            emitter.tableRow(row);
    }
    emitter.jsonArrayEnd(); /// Close "extents".
    if (in_gap)
        emitter.tablePrintf("                     ---\n");
}

/// jemalloc: stats_arena_hpa_shard_sec_print
void statsArenaSmallExtentCachePrint(Emitter & emitter, unsigned i)
{
    size_t small_extent_cache_bytes;
    size_t small_extent_cache_hits;
    size_t small_extent_cache_misses;
    size_t small_extent_cache_deallocate_flush;
    size_t small_extent_cache_deallocate_no_flush;
    size_t small_extent_cache_overfills;
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_sec_bytes", i, &small_extent_cache_bytes);
    emitter.keyValue("sec_bytes", "Bytes in small extent cache", EmitterType::Size, &small_extent_cache_bytes);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_sec_hits", i, &small_extent_cache_hits);
    emitter.keyValue("sec_hits", "Total hits in small extent cache", EmitterType::Size, &small_extent_cache_hits);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_sec_misses", i, &small_extent_cache_misses);
    emitter.keyValue("sec_misses", "Total misses in small extent cache", EmitterType::Size, &small_extent_cache_misses);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_sec_dalloc_noflush", i, &small_extent_cache_deallocate_no_flush);
    emitter.keyValue(
        "sec_dalloc_noflush",
        "Dalloc calls without flush in small extent cache",
        EmitterType::Size,
        &small_extent_cache_deallocate_no_flush);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_sec_dalloc_flush", i, &small_extent_cache_deallocate_flush);
    emitter.keyValue(
        "sec_dalloc_flush", "Dalloc calls with flush in small extent cache", EmitterType::Size, &small_extent_cache_deallocate_flush);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_sec_overfills", i, &small_extent_cache_overfills);
    emitter.keyValue("sec_overfills", "sec_fill calls that went over max_bytes", EmitterType::Size, &small_extent_cache_overfills);
}

/// jemalloc: stats_arena_hpa_shard_counters_print
void statsArenaHugePageShardCountersPrint(Emitter & emitter, unsigned i, uint64_t uptime)
{
    size_t num_page_slabs;
    size_t num_active;
    size_t num_dirty;

    size_t num_page_slabs_non_huge;
    size_t num_active_non_huge;
    size_t num_dirty_non_huge;
    size_t num_retained_non_huge;

    size_t num_page_slabs_huge;
    size_t num_active_huge;
    size_t num_dirty_huge;

    uint64_t num_purge_passes;
    uint64_t num_purges;
    uint64_t num_hugifies;
    uint64_t num_hugify_failures;
    uint64_t num_dehugifies;

    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.npageslabs", i, &num_page_slabs);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.nactive", i, &num_active);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.ndirty", i, &num_dirty);

    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.slabs.npageslabs_nonhuge", i, &num_page_slabs_non_huge);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.slabs.nactive_nonhuge", i, &num_active_non_huge);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.slabs.ndirty_nonhuge", i, &num_dirty_non_huge);
    num_retained_non_huge = num_page_slabs_non_huge * HUGE_PAGE_PAGES - num_active_non_huge - num_dirty_non_huge;

    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.slabs.npageslabs_huge", i, &num_page_slabs_huge);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.slabs.nactive_huge", i, &num_active_huge);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.slabs.ndirty_huge", i, &num_dirty_huge);

    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.npurge_passes", i, &num_purge_passes);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.npurges", i, &num_purges);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.nhugifies", i, &num_hugifies);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.nhugify_failures", i, &num_hugify_failures);
    mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.ndehugifies", i, &num_dehugifies);

    emitter.tablePrintf(
        "HPA shard stats:\n"
        "  Pageslabs: %zu (%zu huge, %zu nonhuge)\n"
        "  Active pages: %zu (%zu huge, %zu nonhuge)\n"
        "  Dirty pages: %zu (%zu huge, %zu nonhuge)\n"
        "  Retained pages: %zu\n"
        "  Purge passes: %" FORMAT_U64 " (%" FORMAT_U64 " / sec)\n"
        "  Purges: %" FORMAT_U64 " (%" FORMAT_U64 " / sec)\n"
        "  Hugeifies: %" FORMAT_U64 " (%" FORMAT_U64 " / sec)\n"
        "  Hugify failures: %" FORMAT_U64 " (%" FORMAT_U64 " / sec)\n"
        "  Dehugifies: %" FORMAT_U64 " (%" FORMAT_U64 " / sec)\n"
        "\n",
        num_page_slabs,
        num_page_slabs_huge,
        num_page_slabs_non_huge,
        num_active,
        num_active_huge,
        num_active_non_huge,
        num_dirty,
        num_dirty_huge,
        num_dirty_non_huge,
        num_retained_non_huge,
        num_purge_passes,
        ratePerSecond(num_purge_passes, uptime),
        num_purges,
        ratePerSecond(num_purges, uptime),
        num_hugifies,
        ratePerSecond(num_hugifies, uptime),
        num_hugify_failures,
        ratePerSecond(num_hugify_failures, uptime),
        num_dehugifies,
        ratePerSecond(num_dehugifies, uptime));

    emitter.jsonKeyValue("npageslabs", EmitterType::Size, &num_page_slabs);
    emitter.jsonKeyValue("nactive", EmitterType::Size, &num_active);
    emitter.jsonKeyValue("ndirty", EmitterType::Size, &num_dirty);

    emitter.jsonKeyValue("npurge_passes", EmitterType::Uint64, &num_purge_passes);
    emitter.jsonKeyValue("npurges", EmitterType::Uint64, &num_purges);
    emitter.jsonKeyValue("nhugifies", EmitterType::Uint64, &num_hugifies);
    emitter.jsonKeyValue("nhugify_failures", EmitterType::Uint64, &num_hugify_failures);
    emitter.jsonKeyValue("ndehugifies", EmitterType::Uint64, &num_dehugifies);

    emitter.jsonObjectKeyValueBegin("slabs");
    emitter.jsonKeyValue("npageslabs_nonhuge", EmitterType::Size, &num_page_slabs_non_huge);
    emitter.jsonKeyValue("nactive_nonhuge", EmitterType::Size, &num_active_non_huge);
    emitter.jsonKeyValue("ndirty_nonhuge", EmitterType::Size, &num_dirty_non_huge);
    emitter.jsonKeyValue("nretained_nonhuge", EmitterType::Size, &num_retained_non_huge);

    emitter.jsonKeyValue("npageslabs_huge", EmitterType::Size, &num_page_slabs_huge);
    emitter.jsonKeyValue("nactive_huge", EmitterType::Size, &num_active_huge);
    emitter.jsonKeyValue("ndirty_huge", EmitterType::Size, &num_dirty_huge);
    emitter.jsonObjectEnd(); /// End "slabs"
}

/// The "full" / "empty" slabs part of `stats_arena_hpa_shard_slabs_print`; `kind` is `full_slabs` or `empty_slabs`.
void statsArenaHugePageShardFullOrEmptySlabsPrint(Emitter & emitter, unsigned i, const char * kind)
{
    const bool full = kind[0] == 'f';
    size_t num_page_slabs_huge;
    size_t num_active_huge;
    size_t num_dirty_huge;

    size_t num_page_slabs_non_huge;
    size_t num_active_non_huge;
    size_t num_dirty_non_huge;
    size_t num_retained_non_huge;

    if (full)
    {
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.full_slabs.npageslabs_huge", i, &num_page_slabs_huge);
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.full_slabs.nactive_huge", i, &num_active_huge);
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.full_slabs.ndirty_huge", i, &num_dirty_huge);

        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.full_slabs.npageslabs_nonhuge", i, &num_page_slabs_non_huge);
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.full_slabs.nactive_nonhuge", i, &num_active_non_huge);
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.full_slabs.ndirty_nonhuge", i, &num_dirty_non_huge);
    }
    else
    {
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.empty_slabs.npageslabs_huge", i, &num_page_slabs_huge);
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.empty_slabs.nactive_huge", i, &num_active_huge);
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.empty_slabs.ndirty_huge", i, &num_dirty_huge);

        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.empty_slabs.npageslabs_nonhuge", i, &num_page_slabs_non_huge);
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.empty_slabs.nactive_nonhuge", i, &num_active_non_huge);
        mallctlGetWithPathComponent2("stats.arenas.0.hpa_shard.empty_slabs.ndirty_nonhuge", i, &num_dirty_non_huge);
    }
    num_retained_non_huge = num_page_slabs_non_huge * HUGE_PAGE_PAGES - num_active_non_huge - num_dirty_non_huge;

    /// jemalloc compatibility: the trailing spaces before the newlines are in the original.
    emitter.tablePrintf(
        "  In %s slabs:\n"
        "      npageslabs: %zu huge, %zu nonhuge\n"
        "      nactive: %zu huge, %zu nonhuge \n"
        "      ndirty: %zu huge, %zu nonhuge \n"
        "      nretained: 0 huge, %zu nonhuge \n",
        full ? "full" : "empty",
        num_page_slabs_huge,
        num_page_slabs_non_huge,
        num_active_huge,
        num_active_non_huge,
        num_dirty_huge,
        num_dirty_non_huge,
        num_retained_non_huge);

    emitter.jsonObjectKeyValueBegin(kind);
    emitter.jsonKeyValue("npageslabs_huge", EmitterType::Size, &num_page_slabs_huge);
    emitter.jsonKeyValue("nactive_huge", EmitterType::Size, &num_active_huge);
    /// jemalloc compatibility: `nactive_huge` is emitted twice and `num_dirty_huge` never.
    emitter.jsonKeyValue("nactive_huge", EmitterType::Size, &num_active_huge);
    emitter.jsonKeyValue("npageslabs_nonhuge", EmitterType::Size, &num_page_slabs_non_huge);
    emitter.jsonKeyValue("nactive_nonhuge", EmitterType::Size, &num_active_non_huge);
    emitter.jsonKeyValue("ndirty_nonhuge", EmitterType::Size, &num_dirty_non_huge);
    emitter.jsonObjectEnd();
}

/// jemalloc: stats_arena_hpa_shard_slabs_print
void statsArenaHugePageShardSlabsPrint(Emitter & emitter, unsigned i)
{
    EmitterRow header_row;
    header_row.init();
    EmitterRow row;
    row.init();

    size_t num_page_slabs_huge;
    size_t num_active_huge;
    size_t num_dirty_huge;

    size_t num_page_slabs_non_huge;
    size_t num_active_non_huge;
    size_t num_dirty_non_huge;
    size_t num_retained_non_huge;

    /// Full slab stats.
    statsArenaHugePageShardFullOrEmptySlabsPrint(emitter, i, "full_slabs");

    /// Next, empty slab stats.
    statsArenaHugePageShardFullOrEmptySlabsPrint(emitter, i, "empty_slabs");

    /// Last, nonfull slab stats.
    ColumnHeader size;
    ColumnHeader idx;
    ColumnHeader num_page_slabs_huge_column;
    ColumnHeader num_active_huge_column;
    ColumnHeader num_dirty_huge_column;
    ColumnHeader num_page_slabs_non_huge_column;
    ColumnHeader num_active_non_huge_column;
    ColumnHeader num_dirty_non_huge_column;
    ColumnHeader num_retained_non_huge_column;

    size.init(row, header_row, "size", nullptr, RIGHT, 20, EmitterType::Size);
    idx.init(row, header_row, "ind", nullptr, RIGHT, 4, EmitterType::Unsigned);
    num_page_slabs_huge_column.init(row, header_row, "npageslabs_huge", nullptr, RIGHT, 16, EmitterType::Size);
    num_active_huge_column.init(row, header_row, "nactive_huge", nullptr, RIGHT, 16, EmitterType::Size);
    num_dirty_huge_column.init(row, header_row, "ndirty_huge", nullptr, RIGHT, 16, EmitterType::Size);
    num_page_slabs_non_huge_column.init(row, header_row, "npageslabs_nonhuge", nullptr, RIGHT, 20, EmitterType::Size);
    num_active_non_huge_column.init(row, header_row, "nactive_nonhuge", nullptr, RIGHT, 20, EmitterType::Size);
    num_dirty_non_huge_column.init(row, header_row, "ndirty_nonhuge", nullptr, RIGHT, 20, EmitterType::Size);
    num_retained_non_huge_column.init(row, header_row, "nretained_nonhuge", nullptr, RIGHT, 20, EmitterType::Size);

    size_t stats_arenas_numeric_path[MALLCTL_MAX_DEPTH];
    mallctlLeafPrepare(stats_arenas_numeric_path, 0, "stats.arenas");
    stats_arenas_numeric_path[2] = i;
    mallctlLeafPrepare(stats_arenas_numeric_path, 3, "hpa_shard.nonfull_slabs");

    emitter.tablePrintf("  In nonfull slabs:\n");
    emitter.tableRow(header_row);
    emitter.jsonArrayKeyValueBegin("nonfull_slabs");
    bool in_gap = false;
    for (PageSizeClassIdx j = 0; j < PAGE_SLAB_SET_NUM_PAGE_SIZES && j < SIZE_CLASS_NUM_PAGE_SIZES; j++)
    {
        stats_arenas_numeric_path[5] = j;

        mallctlLeaf(stats_arenas_numeric_path, 6, "npageslabs_huge", &num_page_slabs_huge);
        mallctlLeaf(stats_arenas_numeric_path, 6, "nactive_huge", &num_active_huge);
        mallctlLeaf(stats_arenas_numeric_path, 6, "ndirty_huge", &num_dirty_huge);

        mallctlLeaf(stats_arenas_numeric_path, 6, "npageslabs_nonhuge", &num_page_slabs_non_huge);
        mallctlLeaf(stats_arenas_numeric_path, 6, "nactive_nonhuge", &num_active_non_huge);
        mallctlLeaf(stats_arenas_numeric_path, 6, "ndirty_nonhuge", &num_dirty_non_huge);
        num_retained_non_huge = num_page_slabs_non_huge * HUGE_PAGE_PAGES - num_active_non_huge - num_dirty_non_huge;

        bool in_gap_prev = in_gap;
        in_gap = (num_page_slabs_huge == 0 && num_page_slabs_non_huge == 0);
        if (in_gap_prev && !in_gap)
            emitter.tablePrintf("                     ---\n");

        size.column.size_value = size_classes::pageSizeClassIdxToSize(j);
        /// jemalloc compatibility: the column has the `unsigned` type but the value is assigned via `size_value`.
        idx.column.size_value = j;
        num_page_slabs_huge_column.column.size_value = num_page_slabs_huge;
        num_active_huge_column.column.size_value = num_active_huge;
        num_dirty_huge_column.column.size_value = num_dirty_huge;
        num_page_slabs_non_huge_column.column.size_value = num_page_slabs_non_huge;
        num_active_non_huge_column.column.size_value = num_active_non_huge;
        num_dirty_non_huge_column.column.size_value = num_dirty_non_huge;
        num_retained_non_huge_column.column.size_value = num_retained_non_huge;
        if (!in_gap)
            emitter.tableRow(row);

        emitter.jsonObjectBegin();
        emitter.jsonKeyValue("npageslabs_huge", EmitterType::Size, &num_page_slabs_huge);
        emitter.jsonKeyValue("nactive_huge", EmitterType::Size, &num_active_huge);
        emitter.jsonKeyValue("ndirty_huge", EmitterType::Size, &num_dirty_huge);
        emitter.jsonKeyValue("npageslabs_nonhuge", EmitterType::Size, &num_page_slabs_non_huge);
        emitter.jsonKeyValue("nactive_nonhuge", EmitterType::Size, &num_active_non_huge);
        emitter.jsonKeyValue("ndirty_nonhuge", EmitterType::Size, &num_dirty_non_huge);
        emitter.jsonObjectEnd();
    }
    emitter.jsonArrayEnd(); /// End "nonfull_slabs"
    if (in_gap)
        emitter.tablePrintf("                     ---\n");
}

/// jemalloc: stats_arena_hpa_shard_print
void statsArenaHugePageShardPrint(Emitter & emitter, unsigned i, uint64_t uptime)
{
    statsArenaSmallExtentCachePrint(emitter, i);

    emitter.jsonObjectKeyValueBegin("hpa_shard");
    statsArenaHugePageShardCountersPrint(emitter, i, uptime);
    statsArenaHugePageShardSlabsPrint(emitter, i);
    emitter.jsonObjectEnd(); /// End "hpa_shard"
}

/// jemalloc: stats_arena_mutexes_print
void statsArenaMutexesPrint(Emitter & emitter, unsigned arena_idx, uint64_t uptime)
{
    EmitterRow row;
    EmitterColumn column_name;
    MutexColumns64 column64;
    MutexColumns32 column32;

    row.init();
    mutexStatsInitColumns(row, "", &column_name, column64, column32);

    emitter.jsonObjectKeyValueBegin("mutexes");
    emitter.tableRow(row);

    size_t stats_arenas_numeric_path[MALLCTL_MAX_DEPTH];
    mallctlLeafPrepare(stats_arenas_numeric_path, 0, "stats.arenas");
    stats_arenas_numeric_path[2] = arena_idx;
    mallctlLeafPrepare(stats_arenas_numeric_path, 3, "mutexes");

    for (unsigned i = 0; i < mutex_profiling_num_arena_mutexes; i++)
    {
        const char * name = mutex_profiling_arena_names[i];
        emitter.jsonObjectKeyValueBegin(name);
        mutexStatsReadNamed(stats_arenas_numeric_path, 4, name, &column_name, column64, column32, uptime);
        mutexStatsEmit(emitter, &row, column64, column32);
        emitter.jsonObjectEnd(); /// Close the mutex dict.
    }
    emitter.jsonObjectEnd(); /// End "mutexes".
}

/// jemalloc: stats_arena_print
ALLOCATOR_COLD void
statsArenaPrint(Emitter & emitter, unsigned i, bool bins, bool large, bool mutex, bool extents, bool huge_page_allocator)
{
    char name[ARENA_NAME_LEN];
    char * name_ptr = name;
    unsigned num_threads;
    const char * sbrk;
    ssize_t dirty_decay_ms;
    ssize_t muzzy_decay_ms;
    size_t page;
    size_t active_pages;
    size_t dirty_pages;
    size_t muzzy_pages;
    uint64_t dirty_num_purge;
    uint64_t dirty_num_madvises;
    uint64_t dirty_purged;
    uint64_t muzzy_num_purge;
    uint64_t muzzy_num_madvises;
    uint64_t muzzy_purged;
    uint64_t uptime;

    mallctlGet("arenas.page", &page);
    if (i != MALLCTL_ARENAS_ALL && i != MALLCTL_ARENAS_DESTROYED)
    {
        mallctlGetWithPathComponent1("arena.0.name", i, &name_ptr);
        emitter.keyValue("name", "name", EmitterType::String, &name_ptr);
    }

    mallctlGetWithPathComponent2("stats.arenas.0.nthreads", i, &num_threads);
    emitter.keyValue("nthreads", "assigned threads", EmitterType::Unsigned, &num_threads);

    mallctlGetWithPathComponent2("stats.arenas.0.uptime", i, &uptime);
    emitter.keyValue("uptime_ns", "uptime", EmitterType::Uint64, &uptime);

    mallctlGetWithPathComponent2("stats.arenas.0.dss", i, &sbrk);
    emitter.keyValue("dss", "dss allocation precedence", EmitterType::String, &sbrk);

    mallctlGetWithPathComponent2("stats.arenas.0.dirty_decay_ms", i, &dirty_decay_ms);
    mallctlGetWithPathComponent2("stats.arenas.0.muzzy_decay_ms", i, &muzzy_decay_ms);
    mallctlGetWithPathComponent2("stats.arenas.0.pactive", i, &active_pages);
    mallctlGetWithPathComponent2("stats.arenas.0.pdirty", i, &dirty_pages);
    mallctlGetWithPathComponent2("stats.arenas.0.pmuzzy", i, &muzzy_pages);
    mallctlGetWithPathComponent2("stats.arenas.0.dirty_npurge", i, &dirty_num_purge);
    mallctlGetWithPathComponent2("stats.arenas.0.dirty_nmadvise", i, &dirty_num_madvises);
    mallctlGetWithPathComponent2("stats.arenas.0.dirty_purged", i, &dirty_purged);
    mallctlGetWithPathComponent2("stats.arenas.0.muzzy_npurge", i, &muzzy_num_purge);
    mallctlGetWithPathComponent2("stats.arenas.0.muzzy_nmadvise", i, &muzzy_num_madvises);
    mallctlGetWithPathComponent2("stats.arenas.0.muzzy_purged", i, &muzzy_purged);

    EmitterRow decay_row;
    decay_row.init();

    /// JSON-style emission.
    emitter.jsonKeyValue("dirty_decay_ms", EmitterType::Ssize, &dirty_decay_ms);
    emitter.jsonKeyValue("muzzy_decay_ms", EmitterType::Ssize, &muzzy_decay_ms);

    emitter.jsonKeyValue("pactive", EmitterType::Size, &active_pages);
    emitter.jsonKeyValue("pdirty", EmitterType::Size, &dirty_pages);
    emitter.jsonKeyValue("pmuzzy", EmitterType::Size, &muzzy_pages);

    emitter.jsonKeyValue("dirty_npurge", EmitterType::Uint64, &dirty_num_purge);
    emitter.jsonKeyValue("dirty_nmadvise", EmitterType::Uint64, &dirty_num_madvises);
    emitter.jsonKeyValue("dirty_purged", EmitterType::Uint64, &dirty_purged);

    emitter.jsonKeyValue("muzzy_npurge", EmitterType::Uint64, &muzzy_num_purge);
    emitter.jsonKeyValue("muzzy_nmadvise", EmitterType::Uint64, &muzzy_num_madvises);
    emitter.jsonKeyValue("muzzy_purged", EmitterType::Uint64, &muzzy_purged);

    /// Table-style emission.
    EmitterColumn column_decay_type;
    columnInit(column_decay_type, decay_row, RIGHT, 9, EmitterType::Title);
    column_decay_type.str_value = "decaying:";

    EmitterColumn column_decay_time;
    columnInit(column_decay_time, decay_row, RIGHT, 6, EmitterType::Title);
    column_decay_time.str_value = "time";

    EmitterColumn column_decay_num_pages;
    columnInit(column_decay_num_pages, decay_row, RIGHT, 13, EmitterType::Title);
    column_decay_num_pages.str_value = "npages";

    EmitterColumn column_decay_sweeps;
    columnInit(column_decay_sweeps, decay_row, RIGHT, 13, EmitterType::Title);
    column_decay_sweeps.str_value = "sweeps";

    EmitterColumn column_decay_madvises;
    columnInit(column_decay_madvises, decay_row, RIGHT, 13, EmitterType::Title);
    column_decay_madvises.str_value = "madvises";

    EmitterColumn column_decay_purged;
    columnInit(column_decay_purged, decay_row, RIGHT, 13, EmitterType::Title);
    column_decay_purged.str_value = "purged";

    /// Title row.
    emitter.tableRow(decay_row);

    /// Dirty row.
    column_decay_type.str_value = "dirty:";

    if (dirty_decay_ms >= 0)
    {
        column_decay_time.type = EmitterType::Ssize;
        column_decay_time.ssize_value = dirty_decay_ms;
    }
    else
    {
        column_decay_time.type = EmitterType::Title;
        column_decay_time.str_value = "N/A";
    }

    column_decay_num_pages.type = EmitterType::Size;
    column_decay_num_pages.size_value = dirty_pages;

    column_decay_sweeps.type = EmitterType::Uint64;
    column_decay_sweeps.uint64_value = dirty_num_purge;

    column_decay_madvises.type = EmitterType::Uint64;
    column_decay_madvises.uint64_value = dirty_num_madvises;

    column_decay_purged.type = EmitterType::Uint64;
    column_decay_purged.uint64_value = dirty_purged;

    emitter.tableRow(decay_row);

    /// Muzzy row.
    column_decay_type.str_value = "muzzy:";

    if (muzzy_decay_ms >= 0)
    {
        column_decay_time.type = EmitterType::Ssize;
        column_decay_time.ssize_value = muzzy_decay_ms;
    }
    else
    {
        column_decay_time.type = EmitterType::Title;
        column_decay_time.str_value = "N/A";
    }

    column_decay_num_pages.type = EmitterType::Size;
    column_decay_num_pages.size_value = muzzy_pages;

    column_decay_sweeps.type = EmitterType::Uint64;
    column_decay_sweeps.uint64_value = muzzy_num_purge;

    column_decay_madvises.type = EmitterType::Uint64;
    column_decay_madvises.uint64_value = muzzy_num_madvises;

    column_decay_purged.type = EmitterType::Uint64;
    column_decay_purged.uint64_value = muzzy_purged;

    emitter.tableRow(decay_row);

    /// Small / large / total allocation counts.
    EmitterRow alloc_count_row;
    alloc_count_row.init();

    EmitterColumn column_count_title;
    columnInit(column_count_title, alloc_count_row, LEFT, 21, EmitterType::Title);
    column_count_title.str_value = "";

    EmitterColumn column_count_allocated;
    columnInit(column_count_allocated, alloc_count_row, RIGHT, 16, EmitterType::Title);
    column_count_allocated.str_value = "allocated";

    EmitterColumn column_count_num_allocations;
    columnInit(column_count_num_allocations, alloc_count_row, RIGHT, 16, EmitterType::Title);
    column_count_num_allocations.str_value = "nmalloc";
    EmitterColumn column_count_num_allocations_per_second;
    columnInit(column_count_num_allocations_per_second, alloc_count_row, RIGHT, 10, EmitterType::Title);
    column_count_num_allocations_per_second.str_value = "(#/sec)";

    EmitterColumn column_count_num_deallocations;
    columnInit(column_count_num_deallocations, alloc_count_row, RIGHT, 16, EmitterType::Title);
    column_count_num_deallocations.str_value = "ndalloc";
    EmitterColumn column_count_num_deallocations_per_second;
    columnInit(column_count_num_deallocations_per_second, alloc_count_row, RIGHT, 10, EmitterType::Title);
    column_count_num_deallocations_per_second.str_value = "(#/sec)";

    EmitterColumn column_count_num_requests;
    columnInit(column_count_num_requests, alloc_count_row, RIGHT, 16, EmitterType::Title);
    column_count_num_requests.str_value = "nrequests";
    EmitterColumn column_count_num_requests_per_second;
    columnInit(column_count_num_requests_per_second, alloc_count_row, RIGHT, 10, EmitterType::Title);
    column_count_num_requests_per_second.str_value = "(#/sec)";

    EmitterColumn column_count_num_fills;
    columnInit(column_count_num_fills, alloc_count_row, RIGHT, 16, EmitterType::Title);
    column_count_num_fills.str_value = "nfill";
    EmitterColumn column_count_num_fills_per_second;
    columnInit(column_count_num_fills_per_second, alloc_count_row, RIGHT, 10, EmitterType::Title);
    column_count_num_fills_per_second.str_value = "(#/sec)";

    EmitterColumn column_count_num_flushes;
    columnInit(column_count_num_flushes, alloc_count_row, RIGHT, 16, EmitterType::Title);
    column_count_num_flushes.str_value = "nflush";
    EmitterColumn column_count_num_flushes_per_second;
    columnInit(column_count_num_flushes_per_second, alloc_count_row, RIGHT, 10, EmitterType::Title);
    column_count_num_flushes_per_second.str_value = "(#/sec)";

    emitter.tableRow(alloc_count_row);

    column_count_num_allocations_per_second.type = EmitterType::Uint64;
    column_count_num_deallocations_per_second.type = EmitterType::Uint64;
    column_count_num_requests_per_second.type = EmitterType::Uint64;
    column_count_num_fills_per_second.type = EmitterType::Uint64;
    column_count_num_flushes_per_second.type = EmitterType::Uint64;

    /// The values of `small` and `large` (jemalloc: `small_allocated`, `small_nmalloc`, ..., `large_nflushes`).
    struct AllocStats
    {
        size_t allocated;
        uint64_t num_allocations;
        uint64_t num_deallocations;
        uint64_t num_requests;
        uint64_t num_fills;
        uint64_t num_flushes;
    };
    AllocStats small_stats;
    AllocStats large_stats;

    /// jemalloc: GET_AND_EMIT_ALLOC_STAT
    auto get_and_emit_size = [&](const char * mallctl_name, const char * json_name, size_t & variable, EmitterColumn & column)
    {
        mallctlGetWithPathComponent2(mallctl_name, i, &variable);
        emitter.jsonKeyValue(json_name, EmitterType::Size, &variable);
        column.type = EmitterType::Size;
        column.size_value = variable;
    };
    auto get_and_emit_uint64 = [&](const char * mallctl_name, const char * json_name, uint64_t & variable, EmitterColumn & column)
    {
        mallctlGetWithPathComponent2(mallctl_name, i, &variable);
        emitter.jsonKeyValue(json_name, EmitterType::Uint64, &variable);
        column.type = EmitterType::Uint64;
        column.uint64_value = variable;
    };

    emitter.jsonObjectKeyValueBegin("small");
    column_count_title.str_value = "small:";

    get_and_emit_size("stats.arenas.0.small.allocated", "allocated", small_stats.allocated, column_count_allocated);
    get_and_emit_uint64("stats.arenas.0.small.nmalloc", "nmalloc", small_stats.num_allocations, column_count_num_allocations);
    column_count_num_allocations_per_second.uint64_value = ratePerSecond(column_count_num_allocations.uint64_value, uptime);
    get_and_emit_uint64("stats.arenas.0.small.ndalloc", "ndalloc", small_stats.num_deallocations, column_count_num_deallocations);
    column_count_num_deallocations_per_second.uint64_value = ratePerSecond(column_count_num_deallocations.uint64_value, uptime);
    get_and_emit_uint64("stats.arenas.0.small.nrequests", "nrequests", small_stats.num_requests, column_count_num_requests);
    column_count_num_requests_per_second.uint64_value = ratePerSecond(column_count_num_requests.uint64_value, uptime);
    get_and_emit_uint64("stats.arenas.0.small.nfills", "nfills", small_stats.num_fills, column_count_num_fills);
    column_count_num_fills_per_second.uint64_value = ratePerSecond(column_count_num_fills.uint64_value, uptime);
    get_and_emit_uint64("stats.arenas.0.small.nflushes", "nflushes", small_stats.num_flushes, column_count_num_flushes);
    column_count_num_flushes_per_second.uint64_value = ratePerSecond(column_count_num_flushes.uint64_value, uptime);

    emitter.tableRow(alloc_count_row);
    emitter.jsonObjectEnd(); /// Close "small".

    emitter.jsonObjectKeyValueBegin("large");
    column_count_title.str_value = "large:";

    get_and_emit_size("stats.arenas.0.large.allocated", "allocated", large_stats.allocated, column_count_allocated);
    get_and_emit_uint64("stats.arenas.0.large.nmalloc", "nmalloc", large_stats.num_allocations, column_count_num_allocations);
    column_count_num_allocations_per_second.uint64_value = ratePerSecond(column_count_num_allocations.uint64_value, uptime);
    get_and_emit_uint64("stats.arenas.0.large.ndalloc", "ndalloc", large_stats.num_deallocations, column_count_num_deallocations);
    column_count_num_deallocations_per_second.uint64_value = ratePerSecond(column_count_num_deallocations.uint64_value, uptime);
    get_and_emit_uint64("stats.arenas.0.large.nrequests", "nrequests", large_stats.num_requests, column_count_num_requests);
    column_count_num_requests_per_second.uint64_value = ratePerSecond(column_count_num_requests.uint64_value, uptime);
    get_and_emit_uint64("stats.arenas.0.large.nfills", "nfills", large_stats.num_fills, column_count_num_fills);
    column_count_num_fills_per_second.uint64_value = ratePerSecond(column_count_num_fills.uint64_value, uptime);
    get_and_emit_uint64("stats.arenas.0.large.nflushes", "nflushes", large_stats.num_flushes, column_count_num_flushes);
    column_count_num_flushes_per_second.uint64_value = ratePerSecond(column_count_num_flushes.uint64_value, uptime);

    emitter.tableRow(alloc_count_row);
    emitter.jsonObjectEnd(); /// Close "large".

    /// Aggregated small + large stats are emitter only in table mode.
    column_count_title.str_value = "total:";
    column_count_allocated.size_value = small_stats.allocated + large_stats.allocated;
    column_count_num_allocations.uint64_value = small_stats.num_allocations + large_stats.num_allocations;
    column_count_num_deallocations.uint64_value = small_stats.num_deallocations + large_stats.num_deallocations;
    column_count_num_requests.uint64_value = small_stats.num_requests + large_stats.num_requests;
    column_count_num_fills.uint64_value = small_stats.num_fills + large_stats.num_fills;
    column_count_num_flushes.uint64_value = small_stats.num_flushes + large_stats.num_flushes;
    column_count_num_allocations_per_second.uint64_value = ratePerSecond(column_count_num_allocations.uint64_value, uptime);
    column_count_num_deallocations_per_second.uint64_value = ratePerSecond(column_count_num_deallocations.uint64_value, uptime);
    column_count_num_requests_per_second.uint64_value = ratePerSecond(column_count_num_requests.uint64_value, uptime);
    column_count_num_fills_per_second.uint64_value = ratePerSecond(column_count_num_fills.uint64_value, uptime);
    column_count_num_flushes_per_second.uint64_value = ratePerSecond(column_count_num_flushes.uint64_value, uptime);
    emitter.tableRow(alloc_count_row);

    EmitterRow memory_count_row;
    memory_count_row.init();

    EmitterColumn memory_count_title;
    memory_count_title.init(memory_count_row);
    memory_count_title.justify = LEFT;
    memory_count_title.width = 21;
    memory_count_title.type = EmitterType::Title;
    memory_count_title.str_value = "";

    EmitterColumn memory_count_value;
    memory_count_value.init(memory_count_row);
    memory_count_value.justify = RIGHT;
    memory_count_value.width = 16;
    memory_count_value.type = EmitterType::Title;
    memory_count_value.str_value = "";

    emitter.tableRow(memory_count_row);
    memory_count_value.type = EmitterType::Size;

    /// Active count in bytes is emitted only in table mode.
    memory_count_title.str_value = "active:";
    memory_count_value.size_value = active_pages * page;
    emitter.tableRow(memory_count_row);

    /// jemalloc: GET_AND_EMIT_MEM_STAT
    struct MemoryStat
    {
        const char * mallctl_name;
        const char * json_name;
        const char * table_name;
    };
    static constexpr MemoryStat memory_stats[] = {
        {"stats.arenas.0.mapped", "mapped", "mapped:"},
        {"stats.arenas.0.retained", "retained", "retained:"},
        {"stats.arenas.0.base", "base", "base:"},
        {"stats.arenas.0.internal", "internal", "internal:"},
        {"stats.arenas.0.metadata_edata", "metadata_edata", "metadata_edata:"},
        {"stats.arenas.0.metadata_rtree", "metadata_rtree", "metadata_rtree:"},
        {"stats.arenas.0.metadata_thp", "metadata_thp", "metadata_thp:"},
        {"stats.arenas.0.tcache_bytes", "tcache_bytes", "tcache_bytes:"},
        {"stats.arenas.0.tcache_stashed_bytes", "tcache_stashed_bytes", "tcache_stashed_bytes:"},
        {"stats.arenas.0.resident", "resident", "resident:"},
        {"stats.arenas.0.abandoned_vm", "abandoned_vm", "abandoned_vm:"},
        {"stats.arenas.0.extent_avail", "extent_avail", "extent_avail:"},
    };
    for (const MemoryStat & stat : memory_stats)
    {
        size_t value;
        mallctlGetWithPathComponent2(stat.mallctl_name, i, &value);
        emitter.jsonKeyValue(stat.json_name, EmitterType::Size, &value);
        memory_count_title.str_value = stat.table_name;
        memory_count_value.size_value = value;
        emitter.tableRow(memory_count_row);
    }

    if (mutex)
        statsArenaMutexesPrint(emitter, i, uptime);
    if (bins)
        statsArenaBinsPrint(emitter, mutex, i, uptime);
    if (large)
        statsArenaLargeExtentsPrint(emitter, i, uptime);
    if (extents)
        statsArenaExtentsPrint(emitter, i);
    if (huge_page_allocator)
        statsArenaHugePageShardPrint(emitter, i, uptime);
}

/// --- General information ----------------------------------------------------------------------------------------

/// The local variables of `stats_general_print` shared by the `OPT_WRITE_*` macros.
struct GeneralPrintVariables
{
    const char * string_value;
    bool bool_value;
    bool bool_value2;
    unsigned unsigned_value;
    uint64_t u64_value;
    int64_t i64_value;
    ssize_t ssize_value;
    ssize_t ssize_value2;
    size_t size_value;
    size_t bool_size = sizeof(bool);
    size_t unsigned_size = sizeof(unsigned);
    size_t size_t_size = sizeof(size_t);
    size_t ssize_t_size = sizeof(ssize_t);
    size_t string_size = sizeof(const char *);
    size_t i64size = sizeof(int64_t);
    size_t u64size = sizeof(uint64_t);
};

/// Prints `opt.<name>` only if the mallctl succeeds. `size` is in/out like in jemalloc (it is shared between calls).
/// jemalloc: OPT_WRITE
template <typename T>
void optionWrite(Emitter & emitter, const char * json_name, const char * table_name, T & variable, size_t & size, EmitterType type)
{
    if (statsMallctl(table_name, static_cast<void *>(&variable), &size, nullptr, 0) == 0)
        emitter.keyValue(json_name, table_name, type, &variable);
}

/// jemalloc: OPT_WRITE_MUTABLE
template <typename T>
void optionWriteMutable(
    Emitter & emitter,
    const char * json_name,
    const char * table_name,
    T & variable1,
    T & variable2,
    size_t & size,
    EmitterType type,
    const char * alternative_name)
{
    if (statsMallctl(table_name, static_cast<void *>(&variable1), &size, nullptr, 0) == 0
        && statsMallctl(alternative_name, static_cast<void *>(&variable2), &size, nullptr, 0) == 0)
        emitter.keyValueNote(json_name, table_name, type, &variable1, alternative_name, type, &variable2);
}

/// jemalloc: stats_general_print
ALLOCATOR_COLD void statsGeneralPrint(Emitter & emitter)
{
    GeneralPrintVariables v;
    uint32_t u32_value;
    size_t u32size = sizeof(uint32_t);

    mallctlGet("version", &v.string_value);
    emitter.keyValue("version", "Version", EmitterType::String, &v.string_value);

    /// config.
    emitter.dictBegin("config", "Build-time option settings");

    /// jemalloc: CONFIG_WRITE_BOOL
    auto config_write_bool = [&](const char * json_name, const char * table_name)
    {
        mallctlGet(table_name, &v.bool_value);
        emitter.keyValue(json_name, table_name, EmitterType::Bool, &v.bool_value);
    };

    config_write_bool("cache_oblivious", "config.cache_oblivious");
    config_write_bool("debug", "config.debug");
    config_write_bool("fill", "config.fill");
    config_write_bool("lazy_lock", "config.lazy_lock");
    const char * config_malloc_conf = config::malloc_conf_default;
    emitter.keyValue("malloc_conf", "config.malloc_conf", EmitterType::String, &config_malloc_conf);

    config_write_bool("opt_safety_checks", "config.opt_safety_checks");
    config_write_bool("prof", "config.prof");
    config_write_bool("prof_libgcc", "config.prof_libgcc");
    config_write_bool("prof_libunwind", "config.prof_libunwind");
    config_write_bool("prof_frameptr", "config.prof_frameptr");
    config_write_bool("stats", "config.stats");
    config_write_bool("utrace", "config.utrace");
    config_write_bool("xmalloc", "config.xmalloc");
    emitter.dictEnd(); /// Close "config" dict.

    /// system.
    emitter.dictBegin("system", "System configuration");

    /// This shows system's THP mode detected at jemalloc's init time. jemalloc does not re-detect the mode even if it
    /// changes after jemalloc's init. It is assumed that system's THP mode is stable during the process's lifetime and
    /// a violation could lead to undefined behavior.
    const char * transparent_huge_pages_mode_name
        = system_transparent_huge_pages_mode_names[static_cast<unsigned>(init_system_transparent_huge_pages_mode)];
    emitter.keyValue("thp_mode", "system.thp_mode", EmitterType::String, &transparent_huge_pages_mode_name);

    emitter.dictEnd(); /// Close "system".

    /// opt.
#define OPTION_WRITE_BOOL(name) optionWrite(emitter, name, "opt." name, v.bool_value, v.bool_size, EmitterType::Bool);
#define OPTION_WRITE_BOOL_MUTABLE(name, alternative_name) \
    optionWriteMutable(emitter, name, "opt." name, v.bool_value, v.bool_value2, v.bool_size, EmitterType::Bool, alternative_name);
#define OPTION_WRITE_UNSIGNED(name) optionWrite(emitter, name, "opt." name, v.unsigned_value, v.unsigned_size, EmitterType::Unsigned);
#define OPTION_WRITE_INT64(name) optionWrite(emitter, name, "opt." name, v.i64_value, v.i64size, EmitterType::Int64);
#define OPTION_WRITE_UINT64(name) optionWrite(emitter, name, "opt." name, v.u64_value, v.u64size, EmitterType::Uint64);
#define OPTION_WRITE_SIZE_T(name) optionWrite(emitter, name, "opt." name, v.size_value, v.size_t_size, EmitterType::Size);
#define OPTION_WRITE_SSIZE_T(name) optionWrite(emitter, name, "opt." name, v.ssize_value, v.ssize_t_size, EmitterType::Ssize);
#define OPTION_WRITE_SSIZE_T_MUTABLE(name, alternative_name) \
    optionWriteMutable(emitter, name, "opt." name, v.ssize_value, v.ssize_value2, v.ssize_t_size, EmitterType::Ssize, alternative_name);
#define OPTION_WRITE_CHAR_PTR(name) optionWrite(emitter, name, "opt." name, v.string_value, v.string_size, EmitterType::String);

    emitter.dictBegin("opt", "Run-time option settings");

    /// opt.malloc_conf.
    ///
    /// Sources are documented in https://jemalloc.net/jemalloc.3.html#tuning
    /// - (Not Included Here) The string specified via --with-malloc-conf, which is already printed out above as
    ///   config.malloc_conf
    /// - (Included) The string pointed to by the global variable malloc_conf
    /// - (Included) The "name" of the file referenced by the symbolic link named /etc/malloc.conf
    /// - (Included) The value of the environment variable MALLOC_CONF
    /// - (Optional, Unofficial) The string pointed to by the global variable malloc_conf_2_conf_harder, which is
    ///   hidden from the public.
    ///
    /// Note: The outputs are strictly ordered by priorities (low -> high).
    ///
    /// jemalloc: MALLOC_CONF_WRITE
    auto malloc_conf_write = [&](const char * mallctl_name, const char * json_name, const char * message)
    {
        if (statsMallctl(mallctl_name, static_cast<void *>(&v.string_value), &v.string_size, nullptr, 0) != 0)
            v.string_value = "";
        emitter.keyValue(json_name, message, EmitterType::String, &v.string_value);
    };

    malloc_conf_write("opt.malloc_conf.global_var", "global_var", "Global variable malloc_conf");
    malloc_conf_write("opt.malloc_conf.symlink", "symlink", "Symbolic link malloc.conf");
    malloc_conf_write("opt.malloc_conf.env_var", "env_var", "Environment variable MALLOC_CONF");
    /// As this config is unofficial, skip the output if it's NULL
    if (statsMallctl("opt.malloc_conf.global_var_2_conf_harder", static_cast<void *>(&v.string_value), &v.string_size, nullptr, 0) == 0)
        emitter.keyValue("global_var_2_conf_harder", "Global variable malloc_conf_2_conf_harder", EmitterType::String, &v.string_value);

    OPTION_WRITE_BOOL("abort")
    OPTION_WRITE_BOOL("abort_conf")
    OPTION_WRITE_BOOL("cache_oblivious")
    OPTION_WRITE_BOOL("confirm_conf")
    OPTION_WRITE_BOOL("experimental_hpa_start_huge_if_thp_always")
    OPTION_WRITE_BOOL("experimental_hpa_enforce_hugify")
    OPTION_WRITE_BOOL("retain")
    OPTION_WRITE_CHAR_PTR("dss")
    OPTION_WRITE_UNSIGNED("narenas")
    OPTION_WRITE_CHAR_PTR("percpu_arena")
    OPTION_WRITE_SIZE_T("oversize_threshold")
    OPTION_WRITE_BOOL("hpa")
    OPTION_WRITE_SIZE_T("hpa_slab_max_alloc")
    OPTION_WRITE_SIZE_T("hpa_hugification_threshold")
    OPTION_WRITE_UINT64("hpa_hugify_delay_ms")
    OPTION_WRITE_BOOL("hpa_hugify_sync")
    OPTION_WRITE_UINT64("hpa_min_purge_interval_ms")
    OPTION_WRITE_SSIZE_T("experimental_hpa_max_purge_nhp")
    if (statsMallctl("opt.hpa_dirty_mult", static_cast<void *>(&u32_value), &u32size, nullptr, 0) == 0)
    {
        /// We cheat a little and "know" the secret meaning of this representation.
        if (u32_value == static_cast<uint32_t>(-1))
        {
            const char * negative1 = "-1";
            emitter.keyValue("hpa_dirty_mult", "opt.hpa_dirty_mult", EmitterType::String, &negative1);
        }
        else
        {
            char buf[fixed_point::BUF_SIZE];
            fixed_point::print(u32_value, buf);
            const char * buf_ptr = buf;
            emitter.keyValue("hpa_dirty_mult", "opt.hpa_dirty_mult", EmitterType::String, &buf_ptr);
        }
    }
    OPTION_WRITE_SIZE_T("hpa_purge_threshold")
    OPTION_WRITE_UINT64("hpa_min_purge_delay_ms")
    OPTION_WRITE_CHAR_PTR("hpa_hugify_style")
    OPTION_WRITE_SIZE_T("hpa_sec_nshards")
    OPTION_WRITE_SIZE_T("hpa_sec_max_alloc")
    OPTION_WRITE_SIZE_T("hpa_sec_max_bytes")
    OPTION_WRITE_SIZE_T("hpa_sec_batch_fill_extra")
    OPTION_WRITE_BOOL("huge_arena_pac_thp")
    OPTION_WRITE_CHAR_PTR("metadata_thp")
    OPTION_WRITE_INT64("mutex_max_spin")
    OPTION_WRITE_BOOL_MUTABLE("background_thread", "background_thread")
    OPTION_WRITE_SSIZE_T_MUTABLE("dirty_decay_ms", "arenas.dirty_decay_ms")
    OPTION_WRITE_SSIZE_T_MUTABLE("muzzy_decay_ms", "arenas.muzzy_decay_ms")
    OPTION_WRITE_SIZE_T("lg_extent_max_active_fit")
    OPTION_WRITE_CHAR_PTR("junk")
    OPTION_WRITE_BOOL("zero")
    OPTION_WRITE_BOOL("utrace")
    OPTION_WRITE_BOOL("xmalloc")
    OPTION_WRITE_BOOL("experimental_infallible_new")
    OPTION_WRITE_BOOL("experimental_tcache_gc")
    OPTION_WRITE_BOOL("tcache")
    OPTION_WRITE_SIZE_T("tcache_max")
    OPTION_WRITE_UNSIGNED("tcache_nslots_small_min")
    OPTION_WRITE_UNSIGNED("tcache_nslots_small_max")
    OPTION_WRITE_UNSIGNED("tcache_nslots_large")
    OPTION_WRITE_SSIZE_T("lg_tcache_nslots_mul")
    OPTION_WRITE_SIZE_T("tcache_gc_incr_bytes")
    OPTION_WRITE_SIZE_T("tcache_gc_delay_bytes")
    OPTION_WRITE_UNSIGNED("lg_tcache_flush_small_div")
    OPTION_WRITE_UNSIGNED("lg_tcache_flush_large_div")
    OPTION_WRITE_UNSIGNED("debug_double_free_max_scan")
    OPTION_WRITE_CHAR_PTR("thp")
    OPTION_WRITE_BOOL("prof")
    OPTION_WRITE_UNSIGNED("prof_bt_max")
    OPTION_WRITE_CHAR_PTR("prof_prefix")
    OPTION_WRITE_BOOL_MUTABLE("prof_active", "prof.active")
    OPTION_WRITE_BOOL_MUTABLE("prof_thread_active_init", "prof.thread_active_init")
    OPTION_WRITE_SSIZE_T_MUTABLE("lg_prof_sample", "prof.lg_sample")
    OPTION_WRITE_BOOL("prof_accum")
    OPTION_WRITE_SSIZE_T("lg_prof_interval")
    OPTION_WRITE_BOOL("prof_gdump")
    OPTION_WRITE_BOOL("prof_final")
    OPTION_WRITE_BOOL("prof_leak")
    OPTION_WRITE_BOOL("prof_leak_error")
    /// jemalloc compatibility: `stats_print` and `stats_print_options` are printed twice.
    OPTION_WRITE_BOOL("stats_print")
    OPTION_WRITE_CHAR_PTR("stats_print_opts")
    OPTION_WRITE_BOOL("stats_print")
    OPTION_WRITE_CHAR_PTR("stats_print_opts")
    OPTION_WRITE_INT64("stats_interval")
    OPTION_WRITE_CHAR_PTR("stats_interval_opts")
    OPTION_WRITE_CHAR_PTR("zero_realloc")
    OPTION_WRITE_SIZE_T("process_madvise_max_batch")
    OPTION_WRITE_BOOL("disable_large_size_classes")

    emitter.dictEnd(); /// Close "opt".

#undef OPTION_WRITE_BOOL
#undef OPTION_WRITE_BOOL_MUTABLE
#undef OPTION_WRITE_UNSIGNED
#undef OPTION_WRITE_INT64
#undef OPTION_WRITE_UINT64
#undef OPTION_WRITE_SIZE_T
#undef OPTION_WRITE_SSIZE_T
#undef OPTION_WRITE_SSIZE_T_MUTABLE
#undef OPTION_WRITE_CHAR_PTR

    /// prof.
    if constexpr (config::profiling)
    {
        emitter.dictBegin("prof", "Profiling settings");

        mallctlGet("prof.thread_active_init", &v.bool_value);
        emitter.keyValue("thread_active_init", "prof.thread_active_init", EmitterType::Bool, &v.bool_value);

        mallctlGet("prof.active", &v.bool_value);
        emitter.keyValue("active", "prof.active", EmitterType::Bool, &v.bool_value);

        mallctlGet("prof.gdump", &v.bool_value);
        emitter.keyValue("gdump", "prof.gdump", EmitterType::Bool, &v.bool_value);

        mallctlGet("prof.interval", &v.u64_value);
        emitter.keyValue("interval", "prof.interval", EmitterType::Uint64, &v.u64_value);

        mallctlGet("prof.lg_sample", &v.ssize_value);
        emitter.keyValue("lg_sample", "prof.lg_sample", EmitterType::Ssize, &v.ssize_value);

        emitter.dictEnd(); /// Close "prof".
    }

    /// arenas.
    /// The json output sticks arena info into an "arenas" dict; the table output puts them at the top-level.
    emitter.jsonObjectKeyValueBegin("arenas");

    mallctlGet("arenas.narenas", &v.unsigned_value);
    emitter.keyValue("narenas", "Arenas", EmitterType::Unsigned, &v.unsigned_value);

    /// Decay settings are emitted only in json mode; in table mode, they're emitted as notes with the opt output,
    /// above.
    mallctlGet("arenas.dirty_decay_ms", &v.ssize_value);
    emitter.jsonKeyValue("dirty_decay_ms", EmitterType::Ssize, &v.ssize_value);

    mallctlGet("arenas.muzzy_decay_ms", &v.ssize_value);
    emitter.jsonKeyValue("muzzy_decay_ms", EmitterType::Ssize, &v.ssize_value);

    mallctlGet("arenas.quantum", &v.size_value);
    emitter.keyValue("quantum", "Quantum size", EmitterType::Size, &v.size_value);

    mallctlGet("arenas.page", &v.size_value);
    emitter.keyValue("page", "Page size", EmitterType::Size, &v.size_value);

    mallctlGet("arenas.hugepage", &v.size_value);
    emitter.keyValue("hugepage", "Hugepage size", EmitterType::Size, &v.size_value);

    if (statsMallctl("arenas.tcache_max", static_cast<void *>(&v.size_value), &v.size_t_size, nullptr, 0) == 0)
        emitter.keyValue("tcache_max", "Maximum thread-cached size class", EmitterType::Size, &v.size_value);

    unsigned arenas_num_bins;
    mallctlGet("arenas.nbins", &arenas_num_bins);
    emitter.keyValue("nbins", "Number of bin size classes", EmitterType::Unsigned, &arenas_num_bins);

    unsigned arenas_num_thread_cache_bins;
    mallctlGet("arenas.nhbins", &arenas_num_thread_cache_bins);
    emitter.keyValue("nhbins", "Number of thread-cache bin size classes", EmitterType::Unsigned, &arenas_num_thread_cache_bins);

    /// We do enough mallctls in a loop that we actually want to omit them (not just omit the printing).
    if (emitter.outputsJSON())
    {
        emitter.jsonArrayKeyValueBegin("bin");
        size_t arenas_bin_numeric_path[MALLCTL_MAX_DEPTH];
        mallctlLeafPrepare(arenas_bin_numeric_path, 0, "arenas.bin");
        for (unsigned i = 0; i < arenas_num_bins; i++)
        {
            arenas_bin_numeric_path[2] = i;
            emitter.jsonObjectBegin();

            mallctlLeaf(arenas_bin_numeric_path, 3, "size", &v.size_value);
            emitter.jsonKeyValue("size", EmitterType::Size, &v.size_value);

            mallctlLeaf(arenas_bin_numeric_path, 3, "nregs", &u32_value);
            emitter.jsonKeyValue("nregs", EmitterType::Uint32, &u32_value);

            mallctlLeaf(arenas_bin_numeric_path, 3, "slab_size", &v.size_value);
            emitter.jsonKeyValue("slab_size", EmitterType::Size, &v.size_value);

            mallctlLeaf(arenas_bin_numeric_path, 3, "nshards", &u32_value);
            emitter.jsonKeyValue("nshards", EmitterType::Uint32, &u32_value);

            emitter.jsonObjectEnd();
        }
        emitter.jsonArrayEnd(); /// Close "bin".
    }

    unsigned num_large_extents;
    mallctlGet("arenas.nlextents", &num_large_extents);
    emitter.keyValue("nlextents", "Number of large size classes", EmitterType::Unsigned, &num_large_extents);

    if (emitter.outputsJSON())
    {
        emitter.jsonArrayKeyValueBegin("lextent");
        size_t arenas_large_extent_numeric_path[MALLCTL_MAX_DEPTH];
        mallctlLeafPrepare(arenas_large_extent_numeric_path, 0, "arenas.lextent");
        for (unsigned i = 0; i < num_large_extents; i++)
        {
            arenas_large_extent_numeric_path[2] = i;
            emitter.jsonObjectBegin();

            mallctlLeaf(arenas_large_extent_numeric_path, 3, "size", &v.size_value);
            emitter.jsonKeyValue("size", EmitterType::Size, &v.size_value);

            emitter.jsonObjectEnd();
        }
        emitter.jsonArrayEnd(); /// Close "lextent".
    }

    emitter.jsonObjectEnd(); /// Close "arenas"
}

/// jemalloc: stats_print_helper
ALLOCATOR_COLD void statsPrintHelper(
    Emitter & emitter,
    bool merged,
    bool destroyed,
    bool unmerged,
    bool bins,
    bool large,
    bool mutex,
    bool extents,
    bool huge_page_allocator)
{
    /// These should be deleted. We keep them around for a while, to aid in the transition to the emitter code.
    size_t allocated;
    size_t active;
    size_t metadata;
    size_t metadata_extent;
    size_t metadata_radix_tree;
    size_t metadata_transparent_huge_pages;
    size_t resident;
    size_t mapped;
    size_t retained;
    size_t num_background_threads;
    size_t zero_reallocs;
    uint64_t background_thread_num_runs;
    uint64_t background_thread_run_interval;

    mallctlGet("stats.allocated", &allocated);
    mallctlGet("stats.active", &active);
    mallctlGet("stats.metadata", &metadata);
    mallctlGet("stats.metadata_edata", &metadata_extent);
    mallctlGet("stats.metadata_rtree", &metadata_radix_tree);
    mallctlGet("stats.metadata_thp", &metadata_transparent_huge_pages);
    mallctlGet("stats.resident", &resident);
    mallctlGet("stats.mapped", &mapped);
    mallctlGet("stats.retained", &retained);

    mallctlGet("stats.zero_reallocs", &zero_reallocs);

    if constexpr (config::background_thread)
    {
        mallctlGet("stats.background_thread.num_threads", &num_background_threads);
        mallctlGet("stats.background_thread.num_runs", &background_thread_num_runs);
        mallctlGet("stats.background_thread.run_interval", &background_thread_run_interval);
    }
    else
    {
        num_background_threads = 0;
        background_thread_num_runs = 0;
        background_thread_run_interval = 0;
    }

    /// Generic global stats.
    emitter.jsonObjectKeyValueBegin("stats");
    emitter.jsonKeyValue("allocated", EmitterType::Size, &allocated);
    emitter.jsonKeyValue("active", EmitterType::Size, &active);
    emitter.jsonKeyValue("metadata", EmitterType::Size, &metadata);
    emitter.jsonKeyValue("metadata_edata", EmitterType::Size, &metadata_extent);
    emitter.jsonKeyValue("metadata_rtree", EmitterType::Size, &metadata_radix_tree);
    emitter.jsonKeyValue("metadata_thp", EmitterType::Size, &metadata_transparent_huge_pages);
    emitter.jsonKeyValue("resident", EmitterType::Size, &resident);
    emitter.jsonKeyValue("mapped", EmitterType::Size, &mapped);
    emitter.jsonKeyValue("retained", EmitterType::Size, &retained);
    emitter.jsonKeyValue("zero_reallocs", EmitterType::Size, &zero_reallocs);

    emitter.tablePrintf(
        "Allocated: %zu, active: %zu, "
        "metadata: %zu (n_thp %zu, edata %zu, rtree %zu), resident: %zu, "
        "mapped: %zu, retained: %zu\n",
        allocated,
        active,
        metadata,
        metadata_transparent_huge_pages,
        metadata_extent,
        metadata_radix_tree,
        resident,
        mapped,
        retained);

    /// Strange behaviors
    emitter.tablePrintf("Count of realloc(non-null-ptr, 0) calls: %zu\n", zero_reallocs);

    /// Background thread stats.
    emitter.jsonObjectKeyValueBegin("background_thread");
    emitter.jsonKeyValue("num_threads", EmitterType::Size, &num_background_threads);
    emitter.jsonKeyValue("num_runs", EmitterType::Uint64, &background_thread_num_runs);
    emitter.jsonKeyValue("run_interval", EmitterType::Uint64, &background_thread_run_interval);
    emitter.jsonObjectEnd(); /// Close "background_thread".

    emitter.tablePrintf(
        "Background threads: %zu, "
        "num_runs: %" FORMAT_U64 ", run_interval: %" FORMAT_U64 " ns\n",
        num_background_threads,
        background_thread_num_runs,
        background_thread_run_interval);

    if (mutex)
    {
        EmitterRow row;
        EmitterColumn name;
        MutexColumns64 column64;
        MutexColumns32 column32;
        uint64_t uptime;

        row.init();
        mutexStatsInitColumns(row, "", &name, column64, column32);

        emitter.tableRow(row);
        emitter.jsonObjectKeyValueBegin("mutexes");

        mallctlGetWithPathComponent2("stats.arenas.0.uptime", 0, &uptime);

        size_t stats_mutexes_numeric_path[MALLCTL_MAX_DEPTH];
        mallctlLeafPrepare(stats_mutexes_numeric_path, 0, "stats.mutexes");
        for (unsigned i = 0; i < mutex_profiling_num_global_mutexes; i++)
        {
            mutexStatsReadNamed(stats_mutexes_numeric_path, 2, mutex_profiling_global_names[i], &name, column64, column32, uptime);
            emitter.jsonObjectKeyValueBegin(mutex_profiling_global_names[i]);
            mutexStatsEmit(emitter, &row, column64, column32);
            emitter.jsonObjectEnd();
        }

        emitter.jsonObjectEnd(); /// Close "mutexes".
    }

    emitter.jsonObjectEnd(); /// Close "stats".

    if (merged || destroyed || unmerged)
    {
        unsigned num_arenas;

        emitter.jsonObjectKeyValueBegin("stats.arenas");

        mallctlGet("arenas.narenas", &num_arenas);
        size_t numeric_path[3];
        size_t numeric_path_length = sizeof(numeric_path) / sizeof(size_t);
        size_t size;
        /// jemalloc: VARIABLE_ARRAY_UNSAFE (a stack array)
        bool * initialized = static_cast<bool *>(__builtin_alloca(num_arenas * sizeof(bool)));
        bool destroyed_initialized;
        unsigned i;
        unsigned num_initialized;

        statsMallctlNameToNumericPathOrAbort("arena.0.initialized", numeric_path, &numeric_path_length);
        for (i = num_initialized = 0; i < num_arenas; i++)
        {
            numeric_path[1] = i;
            size = sizeof(bool);
            statsMallctlByNumericPathOrAbort(numeric_path, numeric_path_length, &initialized[i], &size, nullptr, 0);
            if (initialized[i])
                num_initialized++;
        }
        numeric_path[1] = MALLCTL_ARENAS_DESTROYED;
        size = sizeof(bool);
        statsMallctlByNumericPathOrAbort(numeric_path, numeric_path_length, &destroyed_initialized, &size, nullptr, 0);

        /// Merged stats.
        if (merged && (num_initialized > 1 || !unmerged))
        {
            /// Print merged arena stats.
            emitter.tablePrintf("Merged arenas stats:\n");
            emitter.jsonObjectKeyValueBegin("merged");
            statsArenaPrint(emitter, MALLCTL_ARENAS_ALL, bins, large, mutex, extents, huge_page_allocator);
            emitter.jsonObjectEnd(); /// Close "merged".
        }

        /// Destroyed stats.
        if (destroyed_initialized && destroyed)
        {
            /// Print destroyed arena stats.
            emitter.tablePrintf("Destroyed arenas stats:\n");
            emitter.jsonObjectKeyValueBegin("destroyed");
            statsArenaPrint(emitter, MALLCTL_ARENAS_DESTROYED, bins, large, mutex, extents, huge_page_allocator);
            emitter.jsonObjectEnd(); /// Close "destroyed".
        }

        /// Unmerged stats.
        if (unmerged)
        {
            for (i = 0; i < num_arenas; i++)
            {
                if (initialized[i])
                {
                    char arena_idx_str[20];
                    format(arena_idx_str, sizeof(arena_idx_str), "%u", i);
                    emitter.jsonObjectKeyValueBegin(arena_idx_str);
                    emitter.tablePrintf("arenas[%s]:\n", arena_idx_str);
                    statsArenaPrint(emitter, i, bins, large, mutex, extents, huge_page_allocator);
                    /// Close "<arena-ind>".
                    emitter.jsonObjectEnd();
                }
            }
        }
        emitter.jsonObjectEnd(); /// Close "stats.arenas".
    }
}

}

/// jemalloc: stats_print
void statsPrint(WriteCallback * write_callback, void * callback_argument, const char * options_string)
{
    int error;
    uint64_t epoch;
    size_t u64size;
    /// jemalloc: STATS_PRINT_OPTIONS
    bool json = false;
    bool general = true;
    bool merged = config::stats;
    bool destroyed = config::stats;
    bool unmerged = config::stats;
    bool bins = true;
    bool large = true;
    bool mutex = true;
    bool extents = true;
    bool huge_page_allocator = config::stats;

    /// Refresh stats, in case mallctl() was called by the application.
    ///
    /// Check for OOM here, since refreshing the ctl cache can trigger allocation. In practice, none of the subsequent
    /// mallctl()-related calls in this function will cause OOM if this one succeeds.
    epoch = 1;
    u64size = sizeof(uint64_t);
    error = statsMallctl("epoch", static_cast<void *>(&epoch), &u64size, static_cast<void *>(&epoch), sizeof(uint64_t));
    if (error != 0)
    {
        if (error == EAGAIN)
        {
            writeMessage("<jemalloc>: Memory allocation failure in mallctl(\"epoch\", ...)\n");
            return;
        }
        writeMessage("<jemalloc>: Failure in mallctl(\"epoch\", ...)\n");
        abort();
    }

    if (options_string != nullptr)
    {
        for (unsigned i = 0; options_string[i] != '\0'; i++)
        {
            switch (options_string[i])
            {
                case 'J': json = true; break;
                case 'g': general = false; break;
                case 'm': merged = false; break;
                case 'd': destroyed = false; break;
                case 'a': unmerged = false; break;
                case 'b': bins = false; break;
                case 'l': large = false; break;
                case 'x': mutex = false; break;
                case 'e': extents = false; break;
                case 'h': huge_page_allocator = false; break;
                default:;
            }
        }
    }

    Emitter emitter(json ? EmitterOutput::JSONCompact : EmitterOutput::Table, write_callback, callback_argument);
    emitter.begin();
    emitter.tablePrintf("___ Begin jemalloc statistics ___\n");
    emitter.jsonObjectKeyValueBegin("jemalloc");

    if (general)
        statsGeneralPrint(emitter);
    if constexpr (config::stats)
        statsPrintHelper(emitter, merged, destroyed, unmerged, bins, large, mutex, extents, huge_page_allocator);

    emitter.jsonObjectEnd(); /// Closes the "jemalloc" dict.
    emitter.tablePrintf("--- End jemalloc statistics ---\n");
    emitter.end();
}

}
