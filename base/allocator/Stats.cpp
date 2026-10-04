#include <allocator/Stats.h>

#include <allocator/Arena.h>
#include <allocator/Ctl.h>
#include <allocator/Emitter.h>
#include <allocator/FixedPoint.h>
#include <allocator/Frontend.h>
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
/// Like jemalloc, every value is read through the mallctl machinery (`je_mallctl` semantics for `CTL_GET`, MIB lookups
/// for the loops), in exactly the same order, so that the values and their consistency are identical. The output goes
/// through the `Emitter`, so the sequence of `write_cb` calls is also identical.

namespace jemalloc
{

namespace
{

/// The size of `prof_stats_t` (`prof.stats.{bins,lextents}.<i>.{live,accum}`).
/// jemalloc: prof_stats_t
struct ProfStatsValue
{
    uint64_t req_sum;
    uint64_t count;
};

/// jemalloc: PSSET_NPSIZES (`psset.h`)
constexpr unsigned PSSET_NPSIZES = 64;

/// --- The mallctl access used by the printer (jemalloc: `je_mallctl*`, `xmallctl*` from `ctl.h`). -----------------

/// jemalloc: je_mallctl
int mallctl(const char * name, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (JE_UNLIKELY(mallocInit()))
        return EAGAIN;
    ThreadState & tsd = ThreadState::fetch();
    return ctlByName(tsd, name, oldp, oldlenp, newp, newlen);
}

/// jemalloc: je_mallctlnametomib
int mallctlNameToMib(const char * name, size_t * mibp, size_t * miblenp)
{
    if (JE_UNLIKELY(mallocInit()))
        return EAGAIN;
    ThreadState & tsd = ThreadState::fetch();
    return ctlNameToMib(tsd, name, mibp, miblenp);
}

/// jemalloc: je_mallctlbymib
int mallctlByMib(const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (JE_UNLIKELY(mallocInit()))
        return EAGAIN;
    ThreadState & tsd = ThreadState::fetch();
    return ctlByMib(tsd, mib, miblen, oldp, oldlenp, newp, newlen);
}

/// jemalloc: xmallctl
void xmallctl(const char * name, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (mallctl(name, oldp, oldlenp, newp, newlen) != 0)
    {
        printMessage("<jemalloc>: Failure in xmallctl(\"%s\", ...)\n", name);
        abort();
    }
}

/// jemalloc: xmallctlnametomib
void xmallctlNameToMib(const char * name, size_t * mibp, size_t * miblenp)
{
    if (mallctlNameToMib(name, mibp, miblenp) != 0)
    {
        printMessage("<jemalloc>: Failure in xmallctlnametomib(\"%s\", ...)\n", name);
        abort();
    }
}

/// jemalloc: xmallctlbymib
void xmallctlByMib(const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (mallctlByMib(mib, miblen, oldp, oldlenp, newp, newlen) != 0)
    {
        writeMessage("<jemalloc>: Failure in xmallctlbymib()\n");
        abort();
    }
}

/// jemalloc: xmallctlmibnametomib
void xmallctlMibNameToMib(size_t * mib, size_t miblen, const char * name, size_t * miblenp)
{
    if (ctlMibNameToMib(ThreadState::fetch(), mib, miblen, name, miblenp) != 0)
    {
        writeMessage("<jemalloc>: Failure in ctl_mibnametomib()\n");
        abort();
    }
}

/// jemalloc: xmallctlbymibname
void xmallctlByMibName(
    size_t * mib, size_t miblen, const char * name, size_t * miblenp, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (ctlByMibName(ThreadState::fetch(), mib, miblen, name, miblenp, oldp, oldlenp, newp, newlen) != 0)
    {
        writeMessage("<jemalloc>: Failure in ctl_bymibname()\n");
        abort();
    }
}

/// jemalloc: CTL_GET
template <typename T>
void ctlGet(const char * name, T * v)
{
    size_t sz = sizeof(T);
    xmallctl(name, static_cast<void *>(v), &sz, nullptr, 0);
}

/// jemalloc: CTL_LEAF_PREPARE
void ctlLeafPrepare(size_t * mib, size_t miblen, const char * name)
{
    JE_ASSERT(miblen < CTL_MAX_DEPTH);
    size_t miblen_new = CTL_MAX_DEPTH;
    xmallctlMibNameToMib(mib, miblen, name, &miblen_new);
    JE_ASSERT(miblen_new > miblen);
}

/// jemalloc: CTL_LEAF
template <typename T>
void ctlLeaf(size_t * mib, size_t miblen, const char * leaf, T * v)
{
    JE_ASSERT(miblen < CTL_MAX_DEPTH);
    size_t miblen_new = CTL_MAX_DEPTH;
    size_t sz = sizeof(T);
    xmallctlByMibName(mib, miblen, leaf, &miblen_new, static_cast<void *>(v), &sz, nullptr, 0);
    JE_ASSERT(miblen_new == miblen + 1);
}

/// jemalloc: CTL_MIB_GET
template <typename T>
void ctlMibGet(const char * name, size_t i, T * v, size_t ind)
{
    size_t mib[CTL_MAX_DEPTH];
    size_t miblen = sizeof(mib) / sizeof(size_t);
    size_t sz = sizeof(T);
    xmallctlNameToMib(name, mib, &miblen);
    mib[ind] = i;
    xmallctlByMib(mib, miblen, static_cast<void *>(v), &sz, nullptr, 0);
}

/// jemalloc: CTL_M1_GET
template <typename T>
void ctlM1Get(const char * name, size_t i, T * v)
{
    ctlMibGet(name, i, v, 1);
}

/// jemalloc: CTL_M2_GET
template <typename T>
void ctlM2Get(const char * name, size_t i, T * v)
{
    ctlMibGet(name, i, v, 2);
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
        JE_ASSERT(UINT64_MAX / dividend >= 1000);

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
struct ColHdr
{
    EmitterCol col;
    EmitterCol header;

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
        col.init(row);
        col.justify = justify;
        col.width = width;
        col.type = type;
        header.init(header_row);
        header.justify = justify;
        header.width = width;
        header.type = EmitterType::Title;
        header.str_val = human != nullptr ? human : column_name;
    }
};

/// jemalloc: COL_INIT
void colInit(EmitterCol & col, EmitterRow & row, EmitterJustify justify, int width, EmitterType type)
{
    col.init(row);
    col.justify = justify;
    col.width = width;
    col.type = type;
}

constexpr EmitterJustify LEFT = EmitterJustify::Left;
constexpr EmitterJustify RIGHT = EmitterJustify::Right;

/// --- Mutex statistics -------------------------------------------------------------------------------------------

using MutexCols64 = EmitterCol[mutex_prof_num_uint64_t_counters];
using MutexCols32 = EmitterCol[mutex_prof_num_uint32_t_counters];

/// jemalloc: mutex_stats_init_cols
void mutexStatsInitCols(EmitterRow & row, const char * table_name, EmitterCol * name, MutexCols64 & col_uint64_t, MutexCols32 & col_uint32_t)
{
    if (name != nullptr)
    {
        name->init(row);
        name->justify = LEFT;
        name->width = 21;
        name->type = EmitterType::Title;
        name->str_val = table_name;
    }

    constexpr int WIDTH_uint32_t = 12;
    constexpr int WIDTH_uint64_t = 16;
    for (unsigned k = 0; k < mutex_prof_num_uint64_t_counters; ++k)
    {
        EmitterCol & col = col_uint64_t[k];
        col.init(row);
        col.justify = RIGHT;
        col.width = mutex_prof_uint64_counters[k].derived ? 8 : WIDTH_uint64_t;
        col.type = EmitterType::Title;
        col.str_val = mutex_prof_uint64_counters[k].human;
    }
    for (unsigned k = 0; k < mutex_prof_num_uint32_t_counters; ++k)
    {
        EmitterCol & col = col_uint32_t[k];
        col.init(row);
        col.justify = RIGHT;
        col.width = mutex_prof_uint32_counters[k].derived ? 8 : WIDTH_uint32_t;
        col.type = EmitterType::Title;
        col.str_val = mutex_prof_uint32_counters[k].human;
    }
    col_uint64_t[mutex_counter_total_wait_time_ps].width = 10;
}

/// Reads the counters of one mutex; `mib[0 .. miblen)` is the MIB of the mutex node.
/// jemalloc: the common part of mutex_stats_read_global, mutex_stats_read_arena, mutex_stats_read_arena_bin
void mutexStatsReadCounters(size_t * mib, size_t miblen, MutexCols64 & col_uint64_t, MutexCols32 & col_uint32_t, uint64_t uptime)
{
    for (unsigned k = 0; k < mutex_prof_num_uint64_t_counters; ++k)
    {
        const MutexProfCounterInfo & info = mutex_prof_uint64_counters[k];
        EmitterCol & dst = col_uint64_t[k];
        dst.type = EmitterType::Uint64;
        if (!info.derived)
            ctlLeaf(mib, miblen, info.name, &dst.uint64_val);
        else
            dst.uint64_val = ratePerSecond(col_uint64_t[info.base_counter].uint64_val, uptime);
    }
    for (unsigned k = 0; k < mutex_prof_num_uint32_t_counters; ++k)
    {
        const MutexProfCounterInfo & info = mutex_prof_uint32_counters[k];
        EmitterCol & dst = col_uint32_t[k];
        dst.type = EmitterType::Uint32;
        if (!info.derived)
            ctlLeaf(mib, miblen, info.name, &dst.uint32_val);
        else
            dst.uint32_val = static_cast<uint32_t>(ratePerSecond(col_uint32_t[info.base_counter].uint32_val, uptime));
    }
}

/// jemalloc: mutex_stats_read_global, mutex_stats_read_arena (identical)
void mutexStatsReadNamed(
    size_t * mib, size_t miblen, const char * name, EmitterCol * col_name, MutexCols64 & col_uint64_t, MutexCols32 & col_uint32_t, uint64_t uptime)
{
    ctlLeafPrepare(mib, miblen, name);
    size_t miblen_name = miblen + 1;

    col_name->str_val = name;

    mutexStatsReadCounters(mib, miblen_name, col_uint64_t, col_uint32_t, uptime);
}

/// jemalloc: mutex_stats_read_arena_bin
void mutexStatsReadArenaBin(size_t * mib, size_t miblen, MutexCols64 & col_uint64_t, MutexCols32 & col_uint32_t, uint64_t uptime)
{
    ctlLeafPrepare(mib, miblen, "mutex");
    size_t miblen_mutex = miblen + 1;

    mutexStatsReadCounters(mib, miblen_mutex, col_uint64_t, col_uint32_t, uptime);
}

/// `row` can be null to avoid emitting in table mode.
/// jemalloc: mutex_stats_emit
void mutexStatsEmit(Emitter & emitter, const EmitterRow * row, MutexCols64 & col_uint64_t, MutexCols32 & col_uint32_t)
{
    if (row != nullptr)
        emitter.tableRow(*row);

    for (unsigned k = 0; k < mutex_prof_num_uint64_t_counters; ++k)
        if (!mutex_prof_uint64_counters[k].derived)
            emitter.jsonKv(mutex_prof_uint64_counters[k].name, EmitterType::Uint64, &col_uint64_t[k].uint64_val);
    for (unsigned k = 0; k < mutex_prof_num_uint32_t_counters; ++k)
        if (!mutex_prof_uint32_counters[k].derived)
            emitter.jsonKv(mutex_prof_uint32_counters[k].name, EmitterType::Uint32, &col_uint32_t[k].uint32_val);
}

/// --- Per-arena tables -------------------------------------------------------------------------------------------

/// jemalloc: stats_arena_bins_print
JE_COLD void statsArenaBinsPrint(Emitter & emitter, bool mutex, unsigned i, uint64_t uptime)
{
    size_t page;
    bool in_gap;
    bool in_gap_prev;
    unsigned nbins;
    unsigned j;

    ctlGet("arenas.page", &page);

    ctlGet("arenas.nbins", &nbins);

    EmitterRow header_row;
    header_row.init();

    EmitterRow row;
    row.init();

    bool prof_stats_on = config::prof && opt.prof && opt.prof_stats && i == MALLCTL_ARENAS_ALL;

    ColHdr size;
    ColHdr ind;
    ColHdr allocated;
    ColHdr nmalloc_col;
    ColHdr nmalloc_ps;
    ColHdr ndalloc_col;
    ColHdr ndalloc_ps;
    ColHdr nrequests_col;
    ColHdr nrequests_ps;
    ColHdr prof_live_requested;
    ColHdr prof_live_count;
    ColHdr prof_accum_requested;
    ColHdr prof_accum_count;
    ColHdr nshards_col;
    ColHdr curregs_col;
    ColHdr curslabs_col;
    ColHdr nonfull_slabs_col;
    ColHdr regs;
    ColHdr pgs;
    ColHdr justify_spacer;
    ColHdr util_col;
    ColHdr nfills_col;
    ColHdr nfills_ps;
    ColHdr nflushes_col;
    ColHdr nflushes_ps;
    ColHdr nslabs_col;
    ColHdr nreslabs_col;
    ColHdr nreslabs_ps;

    size.init(row, header_row, "size", nullptr, RIGHT, 20, EmitterType::Size);
    ind.init(row, header_row, "ind", nullptr, RIGHT, 4, EmitterType::Unsigned);
    allocated.init(row, header_row, "allocated", nullptr, RIGHT, 14, EmitterType::Size);
    nmalloc_col.init(row, header_row, "nmalloc", nullptr, RIGHT, 14, EmitterType::Uint64);
    nmalloc_ps.init(row, header_row, "nmalloc_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    ndalloc_col.init(row, header_row, "ndalloc", nullptr, RIGHT, 14, EmitterType::Uint64);
    ndalloc_ps.init(row, header_row, "ndalloc_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    nrequests_col.init(row, header_row, "nrequests", nullptr, RIGHT, 15, EmitterType::Uint64);
    nrequests_ps.init(row, header_row, "nrequests_ps", "(#/sec)", RIGHT, 10, EmitterType::Uint64);
    if (prof_stats_on)
    {
        prof_live_requested.init(row, header_row, "prof_live_requested", nullptr, RIGHT, 21, EmitterType::Uint64);
        prof_live_count.init(row, header_row, "prof_live_count", nullptr, RIGHT, 17, EmitterType::Uint64);
        prof_accum_requested.init(row, header_row, "prof_accum_requested", nullptr, RIGHT, 21, EmitterType::Uint64);
        prof_accum_count.init(row, header_row, "prof_accum_count", nullptr, RIGHT, 17, EmitterType::Uint64);
    }
    nshards_col.init(row, header_row, "nshards", nullptr, RIGHT, 9, EmitterType::Unsigned);
    curregs_col.init(row, header_row, "curregs", nullptr, RIGHT, 13, EmitterType::Size);
    curslabs_col.init(row, header_row, "curslabs", nullptr, RIGHT, 13, EmitterType::Size);
    nonfull_slabs_col.init(row, header_row, "nonfull_slabs", nullptr, RIGHT, 15, EmitterType::Size);
    regs.init(row, header_row, "regs", nullptr, RIGHT, 5, EmitterType::Unsigned);
    pgs.init(row, header_row, "pgs", nullptr, RIGHT, 4, EmitterType::Size);
    /// To buffer a right- and left-justified column.
    justify_spacer.init(row, header_row, "justify_spacer", nullptr, RIGHT, 1, EmitterType::Title);
    util_col.init(row, header_row, "util", nullptr, RIGHT, 6, EmitterType::Title);
    nfills_col.init(row, header_row, "nfills", nullptr, RIGHT, 13, EmitterType::Uint64);
    nfills_ps.init(row, header_row, "nfills_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    nflushes_col.init(row, header_row, "nflushes", nullptr, RIGHT, 13, EmitterType::Uint64);
    nflushes_ps.init(row, header_row, "nflushes_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    nslabs_col.init(row, header_row, "nslabs", nullptr, RIGHT, 13, EmitterType::Uint64);
    nreslabs_col.init(row, header_row, "nreslabs", nullptr, RIGHT, 13, EmitterType::Uint64);
    nreslabs_ps.init(row, header_row, "nreslabs_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);

    /// Don't want to actually print the name.
    justify_spacer.header.str_val = " ";
    justify_spacer.col.str_val = " ";

    MutexCols64 col_mutex64;
    MutexCols32 col_mutex32;

    MutexCols64 header_mutex64;
    MutexCols32 header_mutex32;

    if (mutex)
    {
        mutexStatsInitCols(row, nullptr, nullptr, col_mutex64, col_mutex32);
        mutexStatsInitCols(header_row, nullptr, nullptr, header_mutex64, header_mutex32);
    }

    /// We print a "bins:" header as part of the table row; we need to adjust the header size column to compensate.
    size.header.width -= 5;
    emitter.tablePrintf("bins:");
    emitter.tableRow(header_row);
    emitter.jsonArrayKvBegin("bins");

    size_t stats_arenas_mib[CTL_MAX_DEPTH];
    ctlLeafPrepare(stats_arenas_mib, 0, "stats.arenas");
    stats_arenas_mib[2] = i;
    ctlLeafPrepare(stats_arenas_mib, 3, "bins");

    size_t arenas_bin_mib[CTL_MAX_DEPTH];
    ctlLeafPrepare(arenas_bin_mib, 0, "arenas.bin");

    size_t prof_stats_mib[CTL_MAX_DEPTH];
    if (prof_stats_on)
        ctlLeafPrepare(prof_stats_mib, 0, "prof.stats.bins");

    for (j = 0, in_gap = false; j < nbins; j++)
    {
        uint64_t nslabs;
        size_t reg_size;
        size_t slab_size;
        size_t curregs;
        size_t curslabs;
        size_t nonfull_slabs;
        uint32_t nregs;
        uint32_t nshards;
        uint64_t nmalloc;
        uint64_t ndalloc;
        uint64_t nrequests;
        uint64_t nfills;
        uint64_t nflushes;
        uint64_t nreslabs;
        ProfStatsValue prof_live;
        ProfStatsValue prof_accum;

        stats_arenas_mib[4] = j;
        arenas_bin_mib[2] = j;

        ctlLeaf(stats_arenas_mib, 5, "nslabs", &nslabs);

        if (prof_stats_on)
        {
            prof_stats_mib[3] = j;
            ctlLeaf(prof_stats_mib, 4, "live", &prof_live);
            ctlLeaf(prof_stats_mib, 4, "accum", &prof_accum);
        }

        in_gap_prev = in_gap;
        if (prof_stats_on)
            in_gap = (nslabs == 0 && prof_accum.count == 0);
        else
            in_gap = (nslabs == 0);

        if (in_gap_prev && !in_gap)
            emitter.tablePrintf("                     ---\n");

        if (in_gap && !emitter.outputsJSON())
            continue;

        ctlLeaf(arenas_bin_mib, 3, "size", &reg_size);
        ctlLeaf(arenas_bin_mib, 3, "nregs", &nregs);
        ctlLeaf(arenas_bin_mib, 3, "slab_size", &slab_size);
        ctlLeaf(arenas_bin_mib, 3, "nshards", &nshards);
        ctlLeaf(stats_arenas_mib, 5, "nmalloc", &nmalloc);
        ctlLeaf(stats_arenas_mib, 5, "ndalloc", &ndalloc);
        ctlLeaf(stats_arenas_mib, 5, "curregs", &curregs);
        ctlLeaf(stats_arenas_mib, 5, "nrequests", &nrequests);
        ctlLeaf(stats_arenas_mib, 5, "nfills", &nfills);
        ctlLeaf(stats_arenas_mib, 5, "nflushes", &nflushes);
        ctlLeaf(stats_arenas_mib, 5, "nreslabs", &nreslabs);
        ctlLeaf(stats_arenas_mib, 5, "curslabs", &curslabs);
        ctlLeaf(stats_arenas_mib, 5, "nonfull_slabs", &nonfull_slabs);

        if (mutex)
            mutexStatsReadArenaBin(stats_arenas_mib, 5, col_mutex64, col_mutex32, uptime);

        emitter.jsonObjectBegin();
        emitter.jsonKv("nmalloc", EmitterType::Uint64, &nmalloc);
        emitter.jsonKv("ndalloc", EmitterType::Uint64, &ndalloc);
        emitter.jsonKv("curregs", EmitterType::Size, &curregs);
        emitter.jsonKv("nrequests", EmitterType::Uint64, &nrequests);
        if (prof_stats_on)
        {
            emitter.jsonKv("prof_live_requested", EmitterType::Uint64, &prof_live.req_sum);
            emitter.jsonKv("prof_live_count", EmitterType::Uint64, &prof_live.count);
            emitter.jsonKv("prof_accum_requested", EmitterType::Uint64, &prof_accum.req_sum);
            emitter.jsonKv("prof_accum_count", EmitterType::Uint64, &prof_accum.count);
        }
        emitter.jsonKv("nfills", EmitterType::Uint64, &nfills);
        emitter.jsonKv("nflushes", EmitterType::Uint64, &nflushes);
        emitter.jsonKv("nreslabs", EmitterType::Uint64, &nreslabs);
        emitter.jsonKv("curslabs", EmitterType::Size, &curslabs);
        emitter.jsonKv("nonfull_slabs", EmitterType::Size, &nonfull_slabs);
        if (mutex)
        {
            emitter.jsonObjectKvBegin("mutex");
            mutexStatsEmit(emitter, nullptr, col_mutex64, col_mutex32);
            emitter.jsonObjectEnd();
        }
        emitter.jsonObjectEnd();

        size_t availregs = nregs * curslabs;
        char util[6];
        if (getRateStr(static_cast<uint64_t>(curregs), static_cast<uint64_t>(availregs), util))
        {
            if (availregs == 0)
            {
                format(util, sizeof(util), "1");
            }
            else if (curregs > availregs)
            {
                /// Race detected: the counters were read in separate mallctl calls and concurrent operations happened
                /// in between. In this case no meaningful utilization can be computed.
                format(util, sizeof(util), " race");
            }
            else
            {
                JE_NOT_REACHED();
            }
        }

        size.col.size_val = reg_size;
        ind.col.unsigned_val = j;
        allocated.col.size_val = curregs * reg_size;
        nmalloc_col.col.uint64_val = nmalloc;
        nmalloc_ps.col.uint64_val = ratePerSecond(nmalloc, uptime);
        ndalloc_col.col.uint64_val = ndalloc;
        ndalloc_ps.col.uint64_val = ratePerSecond(ndalloc, uptime);
        nrequests_col.col.uint64_val = nrequests;
        nrequests_ps.col.uint64_val = ratePerSecond(nrequests, uptime);
        if (prof_stats_on)
        {
            prof_live_requested.col.uint64_val = prof_live.req_sum;
            prof_live_count.col.uint64_val = prof_live.count;
            prof_accum_requested.col.uint64_val = prof_accum.req_sum;
            prof_accum_count.col.uint64_val = prof_accum.count;
        }
        nshards_col.col.unsigned_val = nshards;
        curregs_col.col.size_val = curregs;
        curslabs_col.col.size_val = curslabs;
        nonfull_slabs_col.col.size_val = nonfull_slabs;
        regs.col.unsigned_val = nregs;
        pgs.col.size_val = slab_size / page;
        util_col.col.str_val = util;
        nfills_col.col.uint64_val = nfills;
        nfills_ps.col.uint64_val = ratePerSecond(nfills, uptime);
        nflushes_col.col.uint64_val = nflushes;
        nflushes_ps.col.uint64_val = ratePerSecond(nflushes, uptime);
        nslabs_col.col.uint64_val = nslabs;
        nreslabs_col.col.uint64_val = nreslabs;
        nreslabs_ps.col.uint64_val = ratePerSecond(nreslabs, uptime);

        /// Note that mutex columns were initialized above, if mutex == true.

        emitter.tableRow(row);
    }
    emitter.jsonArrayEnd(); /// Close "bins".

    if (in_gap)
        emitter.tablePrintf("                     ---\n");
}

/// jemalloc: stats_arena_lextents_print
JE_COLD void statsArenaLextentsPrint(Emitter & emitter, unsigned i, uint64_t uptime)
{
    unsigned nbins;
    unsigned nlextents;
    unsigned j;
    bool in_gap;
    bool in_gap_prev;

    ctlGet("arenas.nbins", &nbins);
    ctlGet("arenas.nlextents", &nlextents);

    EmitterRow header_row;
    header_row.init();
    EmitterRow row;
    row.init();

    bool prof_stats_on = config::prof && opt.prof && opt.prof_stats && i == MALLCTL_ARENAS_ALL;

    ColHdr size;
    ColHdr ind;
    ColHdr allocated;
    ColHdr nmalloc_col;
    ColHdr nmalloc_ps;
    ColHdr ndalloc_col;
    ColHdr ndalloc_ps;
    ColHdr nrequests_col;
    ColHdr nrequests_ps;
    ColHdr prof_live_requested;
    ColHdr prof_live_count;
    ColHdr prof_accum_requested;
    ColHdr prof_accum_count;
    ColHdr curlextents_col;

    size.init(row, header_row, "size", nullptr, RIGHT, 20, EmitterType::Size);
    ind.init(row, header_row, "ind", nullptr, RIGHT, 4, EmitterType::Unsigned);
    allocated.init(row, header_row, "allocated", nullptr, RIGHT, 13, EmitterType::Size);
    nmalloc_col.init(row, header_row, "nmalloc", nullptr, RIGHT, 13, EmitterType::Uint64);
    nmalloc_ps.init(row, header_row, "nmalloc_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    ndalloc_col.init(row, header_row, "ndalloc", nullptr, RIGHT, 13, EmitterType::Uint64);
    ndalloc_ps.init(row, header_row, "ndalloc_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    nrequests_col.init(row, header_row, "nrequests", nullptr, RIGHT, 13, EmitterType::Uint64);
    nrequests_ps.init(row, header_row, "nrequests_ps", "(#/sec)", RIGHT, 8, EmitterType::Uint64);
    if (prof_stats_on)
    {
        prof_live_requested.init(row, header_row, "prof_live_requested", nullptr, RIGHT, 21, EmitterType::Uint64);
        prof_live_count.init(row, header_row, "prof_live_count", nullptr, RIGHT, 17, EmitterType::Uint64);
        prof_accum_requested.init(row, header_row, "prof_accum_requested", nullptr, RIGHT, 21, EmitterType::Uint64);
        prof_accum_count.init(row, header_row, "prof_accum_count", nullptr, RIGHT, 17, EmitterType::Uint64);
    }
    curlextents_col.init(row, header_row, "curlextents", nullptr, RIGHT, 13, EmitterType::Size);

    /// As with bins, we label the large extents table.
    size.header.width -= 6;
    emitter.tablePrintf("large:");
    emitter.tableRow(header_row);
    emitter.jsonArrayKvBegin("lextents");

    size_t stats_arenas_mib[CTL_MAX_DEPTH];
    ctlLeafPrepare(stats_arenas_mib, 0, "stats.arenas");
    stats_arenas_mib[2] = i;
    ctlLeafPrepare(stats_arenas_mib, 3, "lextents");

    size_t arenas_lextent_mib[CTL_MAX_DEPTH];
    ctlLeafPrepare(arenas_lextent_mib, 0, "arenas.lextent");

    size_t prof_stats_mib[CTL_MAX_DEPTH];
    if (prof_stats_on)
        ctlLeafPrepare(prof_stats_mib, 0, "prof.stats.lextents");

    for (j = 0, in_gap = false; j < nlextents; j++)
    {
        uint64_t nmalloc;
        uint64_t ndalloc;
        uint64_t nrequests;
        size_t lextent_size;
        size_t curlextents;
        ProfStatsValue prof_live;
        ProfStatsValue prof_accum;

        stats_arenas_mib[4] = j;
        arenas_lextent_mib[2] = j;

        ctlLeaf(stats_arenas_mib, 5, "nmalloc", &nmalloc);
        ctlLeaf(stats_arenas_mib, 5, "ndalloc", &ndalloc);
        ctlLeaf(stats_arenas_mib, 5, "nrequests", &nrequests);

        in_gap_prev = in_gap;
        in_gap = (nrequests == 0);

        if (in_gap_prev && !in_gap)
            emitter.tablePrintf("                     ---\n");

        ctlLeaf(arenas_lextent_mib, 3, "size", &lextent_size);
        ctlLeaf(stats_arenas_mib, 5, "curlextents", &curlextents);

        if (prof_stats_on)
        {
            prof_stats_mib[3] = j;
            ctlLeaf(prof_stats_mib, 4, "live", &prof_live);
            ctlLeaf(prof_stats_mib, 4, "accum", &prof_accum);
        }

        emitter.jsonObjectBegin();
        if (prof_stats_on)
        {
            emitter.jsonKv("prof_live_requested", EmitterType::Uint64, &prof_live.req_sum);
            emitter.jsonKv("prof_live_count", EmitterType::Uint64, &prof_live.count);
            emitter.jsonKv("prof_accum_requested", EmitterType::Uint64, &prof_accum.req_sum);
            emitter.jsonKv("prof_accum_count", EmitterType::Uint64, &prof_accum.count);
        }
        emitter.jsonKv("curlextents", EmitterType::Size, &curlextents);
        emitter.jsonObjectEnd();

        size.col.size_val = lextent_size;
        ind.col.unsigned_val = nbins + j;
        allocated.col.size_val = curlextents * lextent_size;
        nmalloc_col.col.uint64_val = nmalloc;
        nmalloc_ps.col.uint64_val = ratePerSecond(nmalloc, uptime);
        ndalloc_col.col.uint64_val = ndalloc;
        ndalloc_ps.col.uint64_val = ratePerSecond(ndalloc, uptime);
        nrequests_col.col.uint64_val = nrequests;
        nrequests_ps.col.uint64_val = ratePerSecond(nrequests, uptime);
        if (prof_stats_on)
        {
            prof_live_requested.col.uint64_val = prof_live.req_sum;
            prof_live_count.col.uint64_val = prof_live.count;
            prof_accum_requested.col.uint64_val = prof_accum.req_sum;
            prof_accum_count.col.uint64_val = prof_accum.count;
        }
        curlextents_col.col.size_val = curlextents;

        if (!in_gap)
            emitter.tableRow(row);
    }
    emitter.jsonArrayEnd(); /// Close "lextents".
    if (in_gap)
        emitter.tablePrintf("                     ---\n");
}

/// jemalloc: stats_arena_extents_print
JE_COLD void statsArenaExtentsPrint(Emitter & emitter, unsigned i)
{
    unsigned j;
    bool in_gap;
    bool in_gap_prev;
    EmitterRow header_row;
    header_row.init();
    EmitterRow row;
    row.init();

    ColHdr size;
    ColHdr ind;
    ColHdr ndirty_col;
    ColHdr dirty_col;
    ColHdr nmuzzy_col;
    ColHdr muzzy_col;
    ColHdr nretained_col;
    ColHdr retained_col;
    ColHdr ntotal_col;
    ColHdr total_col;

    size.init(row, header_row, "size", nullptr, RIGHT, 20, EmitterType::Size);
    ind.init(row, header_row, "ind", nullptr, RIGHT, 4, EmitterType::Unsigned);
    ndirty_col.init(row, header_row, "ndirty", nullptr, RIGHT, 13, EmitterType::Size);
    dirty_col.init(row, header_row, "dirty", nullptr, RIGHT, 13, EmitterType::Size);
    nmuzzy_col.init(row, header_row, "nmuzzy", nullptr, RIGHT, 13, EmitterType::Size);
    muzzy_col.init(row, header_row, "muzzy", nullptr, RIGHT, 13, EmitterType::Size);
    nretained_col.init(row, header_row, "nretained", nullptr, RIGHT, 13, EmitterType::Size);
    retained_col.init(row, header_row, "retained", nullptr, RIGHT, 13, EmitterType::Size);
    ntotal_col.init(row, header_row, "ntotal", nullptr, RIGHT, 13, EmitterType::Size);
    total_col.init(row, header_row, "total", nullptr, RIGHT, 13, EmitterType::Size);

    /// Label this section.
    size.header.width -= 8;
    emitter.tablePrintf("extents:");
    emitter.tableRow(header_row);
    emitter.jsonArrayKvBegin("extents");

    size_t stats_arenas_mib[CTL_MAX_DEPTH];
    ctlLeafPrepare(stats_arenas_mib, 0, "stats.arenas");
    stats_arenas_mib[2] = i;
    ctlLeafPrepare(stats_arenas_mib, 3, "extents");

    in_gap = false;
    for (j = 0; j < SC_NPSIZES; j++)
    {
        size_t ndirty;
        size_t nmuzzy;
        size_t nretained;
        size_t total;
        size_t dirty_bytes;
        size_t muzzy_bytes;
        size_t retained_bytes;
        size_t total_bytes;
        stats_arenas_mib[4] = j;

        ctlLeaf(stats_arenas_mib, 5, "ndirty", &ndirty);
        ctlLeaf(stats_arenas_mib, 5, "nmuzzy", &nmuzzy);
        ctlLeaf(stats_arenas_mib, 5, "nretained", &nretained);
        ctlLeaf(stats_arenas_mib, 5, "dirty_bytes", &dirty_bytes);
        ctlLeaf(stats_arenas_mib, 5, "muzzy_bytes", &muzzy_bytes);
        ctlLeaf(stats_arenas_mib, 5, "retained_bytes", &retained_bytes);

        total = ndirty + nmuzzy + nretained;
        total_bytes = dirty_bytes + muzzy_bytes + retained_bytes;

        in_gap_prev = in_gap;
        in_gap = (total == 0);

        if (in_gap_prev && !in_gap)
            emitter.tablePrintf("                     ---\n");

        emitter.jsonObjectBegin();
        emitter.jsonKv("ndirty", EmitterType::Size, &ndirty);
        emitter.jsonKv("nmuzzy", EmitterType::Size, &nmuzzy);
        emitter.jsonKv("nretained", EmitterType::Size, &nretained);

        emitter.jsonKv("dirty_bytes", EmitterType::Size, &dirty_bytes);
        emitter.jsonKv("muzzy_bytes", EmitterType::Size, &muzzy_bytes);
        emitter.jsonKv("retained_bytes", EmitterType::Size, &retained_bytes);
        emitter.jsonObjectEnd();

        size.col.size_val = sz::pind2sz(j);
        /// jemalloc compatibility: the column has the `unsigned` type but the value is assigned via `size_val`.
        ind.col.size_val = j;
        ndirty_col.col.size_val = ndirty;
        dirty_col.col.size_val = dirty_bytes;
        nmuzzy_col.col.size_val = nmuzzy;
        muzzy_col.col.size_val = muzzy_bytes;
        nretained_col.col.size_val = nretained;
        retained_col.col.size_val = retained_bytes;
        ntotal_col.col.size_val = total;
        total_col.col.size_val = total_bytes;

        if (!in_gap)
            emitter.tableRow(row);
    }
    emitter.jsonArrayEnd(); /// Close "extents".
    if (in_gap)
        emitter.tablePrintf("                     ---\n");
}

/// jemalloc: stats_arena_hpa_shard_sec_print
void statsArenaHpaShardSecPrint(Emitter & emitter, unsigned i)
{
    size_t sec_bytes;
    size_t sec_hits;
    size_t sec_misses;
    size_t sec_dalloc_flush;
    size_t sec_dalloc_noflush;
    size_t sec_overfills;
    ctlM2Get("stats.arenas.0.hpa_sec_bytes", i, &sec_bytes);
    emitter.kv("sec_bytes", "Bytes in small extent cache", EmitterType::Size, &sec_bytes);
    ctlM2Get("stats.arenas.0.hpa_sec_hits", i, &sec_hits);
    emitter.kv("sec_hits", "Total hits in small extent cache", EmitterType::Size, &sec_hits);
    ctlM2Get("stats.arenas.0.hpa_sec_misses", i, &sec_misses);
    emitter.kv("sec_misses", "Total misses in small extent cache", EmitterType::Size, &sec_misses);
    ctlM2Get("stats.arenas.0.hpa_sec_dalloc_noflush", i, &sec_dalloc_noflush);
    emitter.kv("sec_dalloc_noflush", "Dalloc calls without flush in small extent cache", EmitterType::Size, &sec_dalloc_noflush);
    ctlM2Get("stats.arenas.0.hpa_sec_dalloc_flush", i, &sec_dalloc_flush);
    emitter.kv("sec_dalloc_flush", "Dalloc calls with flush in small extent cache", EmitterType::Size, &sec_dalloc_flush);
    ctlM2Get("stats.arenas.0.hpa_sec_overfills", i, &sec_overfills);
    emitter.kv("sec_overfills", "sec_fill calls that went over max_bytes", EmitterType::Size, &sec_overfills);
}

/// jemalloc: stats_arena_hpa_shard_counters_print
void statsArenaHpaShardCountersPrint(Emitter & emitter, unsigned i, uint64_t uptime)
{
    size_t npageslabs;
    size_t nactive;
    size_t ndirty;

    size_t npageslabs_nonhuge;
    size_t nactive_nonhuge;
    size_t ndirty_nonhuge;
    size_t nretained_nonhuge;

    size_t npageslabs_huge;
    size_t nactive_huge;
    size_t ndirty_huge;

    uint64_t npurge_passes;
    uint64_t npurges;
    uint64_t nhugifies;
    uint64_t nhugify_failures;
    uint64_t ndehugifies;

    ctlM2Get("stats.arenas.0.hpa_shard.npageslabs", i, &npageslabs);
    ctlM2Get("stats.arenas.0.hpa_shard.nactive", i, &nactive);
    ctlM2Get("stats.arenas.0.hpa_shard.ndirty", i, &ndirty);

    ctlM2Get("stats.arenas.0.hpa_shard.slabs.npageslabs_nonhuge", i, &npageslabs_nonhuge);
    ctlM2Get("stats.arenas.0.hpa_shard.slabs.nactive_nonhuge", i, &nactive_nonhuge);
    ctlM2Get("stats.arenas.0.hpa_shard.slabs.ndirty_nonhuge", i, &ndirty_nonhuge);
    nretained_nonhuge = npageslabs_nonhuge * HUGEPAGE_PAGES - nactive_nonhuge - ndirty_nonhuge;

    ctlM2Get("stats.arenas.0.hpa_shard.slabs.npageslabs_huge", i, &npageslabs_huge);
    ctlM2Get("stats.arenas.0.hpa_shard.slabs.nactive_huge", i, &nactive_huge);
    ctlM2Get("stats.arenas.0.hpa_shard.slabs.ndirty_huge", i, &ndirty_huge);

    ctlM2Get("stats.arenas.0.hpa_shard.npurge_passes", i, &npurge_passes);
    ctlM2Get("stats.arenas.0.hpa_shard.npurges", i, &npurges);
    ctlM2Get("stats.arenas.0.hpa_shard.nhugifies", i, &nhugifies);
    ctlM2Get("stats.arenas.0.hpa_shard.nhugify_failures", i, &nhugify_failures);
    ctlM2Get("stats.arenas.0.hpa_shard.ndehugifies", i, &ndehugifies);

    emitter.tablePrintf(
        "HPA shard stats:\n"
        "  Pageslabs: %zu (%zu huge, %zu nonhuge)\n"
        "  Active pages: %zu (%zu huge, %zu nonhuge)\n"
        "  Dirty pages: %zu (%zu huge, %zu nonhuge)\n"
        "  Retained pages: %zu\n"
        "  Purge passes: %" FMTu64 " (%" FMTu64 " / sec)\n"
        "  Purges: %" FMTu64 " (%" FMTu64 " / sec)\n"
        "  Hugeifies: %" FMTu64 " (%" FMTu64 " / sec)\n"
        "  Hugify failures: %" FMTu64 " (%" FMTu64 " / sec)\n"
        "  Dehugifies: %" FMTu64 " (%" FMTu64 " / sec)\n"
        "\n",
        npageslabs,
        npageslabs_huge,
        npageslabs_nonhuge,
        nactive,
        nactive_huge,
        nactive_nonhuge,
        ndirty,
        ndirty_huge,
        ndirty_nonhuge,
        nretained_nonhuge,
        npurge_passes,
        ratePerSecond(npurge_passes, uptime),
        npurges,
        ratePerSecond(npurges, uptime),
        nhugifies,
        ratePerSecond(nhugifies, uptime),
        nhugify_failures,
        ratePerSecond(nhugify_failures, uptime),
        ndehugifies,
        ratePerSecond(ndehugifies, uptime));

    emitter.jsonKv("npageslabs", EmitterType::Size, &npageslabs);
    emitter.jsonKv("nactive", EmitterType::Size, &nactive);
    emitter.jsonKv("ndirty", EmitterType::Size, &ndirty);

    emitter.jsonKv("npurge_passes", EmitterType::Uint64, &npurge_passes);
    emitter.jsonKv("npurges", EmitterType::Uint64, &npurges);
    emitter.jsonKv("nhugifies", EmitterType::Uint64, &nhugifies);
    emitter.jsonKv("nhugify_failures", EmitterType::Uint64, &nhugify_failures);
    emitter.jsonKv("ndehugifies", EmitterType::Uint64, &ndehugifies);

    emitter.jsonObjectKvBegin("slabs");
    emitter.jsonKv("npageslabs_nonhuge", EmitterType::Size, &npageslabs_nonhuge);
    emitter.jsonKv("nactive_nonhuge", EmitterType::Size, &nactive_nonhuge);
    emitter.jsonKv("ndirty_nonhuge", EmitterType::Size, &ndirty_nonhuge);
    emitter.jsonKv("nretained_nonhuge", EmitterType::Size, &nretained_nonhuge);

    emitter.jsonKv("npageslabs_huge", EmitterType::Size, &npageslabs_huge);
    emitter.jsonKv("nactive_huge", EmitterType::Size, &nactive_huge);
    emitter.jsonKv("ndirty_huge", EmitterType::Size, &ndirty_huge);
    emitter.jsonObjectEnd(); /// End "slabs"
}

/// The "full" / "empty" slabs part of `stats_arena_hpa_shard_slabs_print`; `kind` is `full_slabs` or `empty_slabs`.
void statsArenaHpaShardFullOrEmptySlabsPrint(Emitter & emitter, unsigned i, const char * kind)
{
    const bool full = kind[0] == 'f';
    size_t npageslabs_huge;
    size_t nactive_huge;
    size_t ndirty_huge;

    size_t npageslabs_nonhuge;
    size_t nactive_nonhuge;
    size_t ndirty_nonhuge;
    size_t nretained_nonhuge;

    if (full)
    {
        ctlM2Get("stats.arenas.0.hpa_shard.full_slabs.npageslabs_huge", i, &npageslabs_huge);
        ctlM2Get("stats.arenas.0.hpa_shard.full_slabs.nactive_huge", i, &nactive_huge);
        ctlM2Get("stats.arenas.0.hpa_shard.full_slabs.ndirty_huge", i, &ndirty_huge);

        ctlM2Get("stats.arenas.0.hpa_shard.full_slabs.npageslabs_nonhuge", i, &npageslabs_nonhuge);
        ctlM2Get("stats.arenas.0.hpa_shard.full_slabs.nactive_nonhuge", i, &nactive_nonhuge);
        ctlM2Get("stats.arenas.0.hpa_shard.full_slabs.ndirty_nonhuge", i, &ndirty_nonhuge);
    }
    else
    {
        ctlM2Get("stats.arenas.0.hpa_shard.empty_slabs.npageslabs_huge", i, &npageslabs_huge);
        ctlM2Get("stats.arenas.0.hpa_shard.empty_slabs.nactive_huge", i, &nactive_huge);
        ctlM2Get("stats.arenas.0.hpa_shard.empty_slabs.ndirty_huge", i, &ndirty_huge);

        ctlM2Get("stats.arenas.0.hpa_shard.empty_slabs.npageslabs_nonhuge", i, &npageslabs_nonhuge);
        ctlM2Get("stats.arenas.0.hpa_shard.empty_slabs.nactive_nonhuge", i, &nactive_nonhuge);
        ctlM2Get("stats.arenas.0.hpa_shard.empty_slabs.ndirty_nonhuge", i, &ndirty_nonhuge);
    }
    nretained_nonhuge = npageslabs_nonhuge * HUGEPAGE_PAGES - nactive_nonhuge - ndirty_nonhuge;

    /// jemalloc compatibility: the trailing spaces before the newlines are in the original.
    emitter.tablePrintf(
        "  In %s slabs:\n"
        "      npageslabs: %zu huge, %zu nonhuge\n"
        "      nactive: %zu huge, %zu nonhuge \n"
        "      ndirty: %zu huge, %zu nonhuge \n"
        "      nretained: 0 huge, %zu nonhuge \n",
        full ? "full" : "empty",
        npageslabs_huge,
        npageslabs_nonhuge,
        nactive_huge,
        nactive_nonhuge,
        ndirty_huge,
        ndirty_nonhuge,
        nretained_nonhuge);

    emitter.jsonObjectKvBegin(kind);
    emitter.jsonKv("npageslabs_huge", EmitterType::Size, &npageslabs_huge);
    emitter.jsonKv("nactive_huge", EmitterType::Size, &nactive_huge);
    /// jemalloc compatibility: `nactive_huge` is emitted twice and `ndirty_huge` never.
    emitter.jsonKv("nactive_huge", EmitterType::Size, &nactive_huge);
    emitter.jsonKv("npageslabs_nonhuge", EmitterType::Size, &npageslabs_nonhuge);
    emitter.jsonKv("nactive_nonhuge", EmitterType::Size, &nactive_nonhuge);
    emitter.jsonKv("ndirty_nonhuge", EmitterType::Size, &ndirty_nonhuge);
    emitter.jsonObjectEnd();
}

/// jemalloc: stats_arena_hpa_shard_slabs_print
void statsArenaHpaShardSlabsPrint(Emitter & emitter, unsigned i)
{
    EmitterRow header_row;
    header_row.init();
    EmitterRow row;
    row.init();

    size_t npageslabs_huge;
    size_t nactive_huge;
    size_t ndirty_huge;

    size_t npageslabs_nonhuge;
    size_t nactive_nonhuge;
    size_t ndirty_nonhuge;
    size_t nretained_nonhuge;

    /// Full slab stats.
    statsArenaHpaShardFullOrEmptySlabsPrint(emitter, i, "full_slabs");

    /// Next, empty slab stats.
    statsArenaHpaShardFullOrEmptySlabsPrint(emitter, i, "empty_slabs");

    /// Last, nonfull slab stats.
    ColHdr size;
    ColHdr ind;
    ColHdr npageslabs_huge_col;
    ColHdr nactive_huge_col;
    ColHdr ndirty_huge_col;
    ColHdr npageslabs_nonhuge_col;
    ColHdr nactive_nonhuge_col;
    ColHdr ndirty_nonhuge_col;
    ColHdr nretained_nonhuge_col;

    size.init(row, header_row, "size", nullptr, RIGHT, 20, EmitterType::Size);
    ind.init(row, header_row, "ind", nullptr, RIGHT, 4, EmitterType::Unsigned);
    npageslabs_huge_col.init(row, header_row, "npageslabs_huge", nullptr, RIGHT, 16, EmitterType::Size);
    nactive_huge_col.init(row, header_row, "nactive_huge", nullptr, RIGHT, 16, EmitterType::Size);
    ndirty_huge_col.init(row, header_row, "ndirty_huge", nullptr, RIGHT, 16, EmitterType::Size);
    npageslabs_nonhuge_col.init(row, header_row, "npageslabs_nonhuge", nullptr, RIGHT, 20, EmitterType::Size);
    nactive_nonhuge_col.init(row, header_row, "nactive_nonhuge", nullptr, RIGHT, 20, EmitterType::Size);
    ndirty_nonhuge_col.init(row, header_row, "ndirty_nonhuge", nullptr, RIGHT, 20, EmitterType::Size);
    nretained_nonhuge_col.init(row, header_row, "nretained_nonhuge", nullptr, RIGHT, 20, EmitterType::Size);

    size_t stats_arenas_mib[CTL_MAX_DEPTH];
    ctlLeafPrepare(stats_arenas_mib, 0, "stats.arenas");
    stats_arenas_mib[2] = i;
    ctlLeafPrepare(stats_arenas_mib, 3, "hpa_shard.nonfull_slabs");

    emitter.tablePrintf("  In nonfull slabs:\n");
    emitter.tableRow(header_row);
    emitter.jsonArrayKvBegin("nonfull_slabs");
    bool in_gap = false;
    for (pszind_t j = 0; j < PSSET_NPSIZES && j < SC_NPSIZES; j++)
    {
        stats_arenas_mib[5] = j;

        ctlLeaf(stats_arenas_mib, 6, "npageslabs_huge", &npageslabs_huge);
        ctlLeaf(stats_arenas_mib, 6, "nactive_huge", &nactive_huge);
        ctlLeaf(stats_arenas_mib, 6, "ndirty_huge", &ndirty_huge);

        ctlLeaf(stats_arenas_mib, 6, "npageslabs_nonhuge", &npageslabs_nonhuge);
        ctlLeaf(stats_arenas_mib, 6, "nactive_nonhuge", &nactive_nonhuge);
        ctlLeaf(stats_arenas_mib, 6, "ndirty_nonhuge", &ndirty_nonhuge);
        nretained_nonhuge = npageslabs_nonhuge * HUGEPAGE_PAGES - nactive_nonhuge - ndirty_nonhuge;

        bool in_gap_prev = in_gap;
        in_gap = (npageslabs_huge == 0 && npageslabs_nonhuge == 0);
        if (in_gap_prev && !in_gap)
            emitter.tablePrintf("                     ---\n");

        size.col.size_val = sz::pind2sz(j);
        /// jemalloc compatibility: the column has the `unsigned` type but the value is assigned via `size_val`.
        ind.col.size_val = j;
        npageslabs_huge_col.col.size_val = npageslabs_huge;
        nactive_huge_col.col.size_val = nactive_huge;
        ndirty_huge_col.col.size_val = ndirty_huge;
        npageslabs_nonhuge_col.col.size_val = npageslabs_nonhuge;
        nactive_nonhuge_col.col.size_val = nactive_nonhuge;
        ndirty_nonhuge_col.col.size_val = ndirty_nonhuge;
        nretained_nonhuge_col.col.size_val = nretained_nonhuge;
        if (!in_gap)
            emitter.tableRow(row);

        emitter.jsonObjectBegin();
        emitter.jsonKv("npageslabs_huge", EmitterType::Size, &npageslabs_huge);
        emitter.jsonKv("nactive_huge", EmitterType::Size, &nactive_huge);
        emitter.jsonKv("ndirty_huge", EmitterType::Size, &ndirty_huge);
        emitter.jsonKv("npageslabs_nonhuge", EmitterType::Size, &npageslabs_nonhuge);
        emitter.jsonKv("nactive_nonhuge", EmitterType::Size, &nactive_nonhuge);
        emitter.jsonKv("ndirty_nonhuge", EmitterType::Size, &ndirty_nonhuge);
        emitter.jsonObjectEnd();
    }
    emitter.jsonArrayEnd(); /// End "nonfull_slabs"
    if (in_gap)
        emitter.tablePrintf("                     ---\n");
}

/// jemalloc: stats_arena_hpa_shard_print
void statsArenaHpaShardPrint(Emitter & emitter, unsigned i, uint64_t uptime)
{
    statsArenaHpaShardSecPrint(emitter, i);

    emitter.jsonObjectKvBegin("hpa_shard");
    statsArenaHpaShardCountersPrint(emitter, i, uptime);
    statsArenaHpaShardSlabsPrint(emitter, i);
    emitter.jsonObjectEnd(); /// End "hpa_shard"
}

/// jemalloc: stats_arena_mutexes_print
void statsArenaMutexesPrint(Emitter & emitter, unsigned arena_ind, uint64_t uptime)
{
    EmitterRow row;
    EmitterCol col_name;
    MutexCols64 col64;
    MutexCols32 col32;

    row.init();
    mutexStatsInitCols(row, "", &col_name, col64, col32);

    emitter.jsonObjectKvBegin("mutexes");
    emitter.tableRow(row);

    size_t stats_arenas_mib[CTL_MAX_DEPTH];
    ctlLeafPrepare(stats_arenas_mib, 0, "stats.arenas");
    stats_arenas_mib[2] = arena_ind;
    ctlLeafPrepare(stats_arenas_mib, 3, "mutexes");

    for (unsigned i = 0; i < mutex_prof_num_arena_mutexes; i++)
    {
        const char * name = mutex_prof_arena_names[i];
        emitter.jsonObjectKvBegin(name);
        mutexStatsReadNamed(stats_arenas_mib, 4, name, &col_name, col64, col32, uptime);
        mutexStatsEmit(emitter, &row, col64, col32);
        emitter.jsonObjectEnd(); /// Close the mutex dict.
    }
    emitter.jsonObjectEnd(); /// End "mutexes".
}

/// jemalloc: stats_arena_print
JE_COLD void statsArenaPrint(Emitter & emitter, unsigned i, bool bins, bool large, bool mutex, bool extents, bool hpa)
{
    char name[ARENA_NAME_LEN];
    char * namep = name;
    unsigned nthreads;
    const char * dss;
    ssize_t dirty_decay_ms;
    ssize_t muzzy_decay_ms;
    size_t page;
    size_t pactive;
    size_t pdirty;
    size_t pmuzzy;
    uint64_t dirty_npurge;
    uint64_t dirty_nmadvise;
    uint64_t dirty_purged;
    uint64_t muzzy_npurge;
    uint64_t muzzy_nmadvise;
    uint64_t muzzy_purged;
    uint64_t uptime;

    ctlGet("arenas.page", &page);
    if (i != MALLCTL_ARENAS_ALL && i != MALLCTL_ARENAS_DESTROYED)
    {
        ctlM1Get("arena.0.name", i, &namep);
        emitter.kv("name", "name", EmitterType::String, &namep);
    }

    ctlM2Get("stats.arenas.0.nthreads", i, &nthreads);
    emitter.kv("nthreads", "assigned threads", EmitterType::Unsigned, &nthreads);

    ctlM2Get("stats.arenas.0.uptime", i, &uptime);
    emitter.kv("uptime_ns", "uptime", EmitterType::Uint64, &uptime);

    ctlM2Get("stats.arenas.0.dss", i, &dss);
    emitter.kv("dss", "dss allocation precedence", EmitterType::String, &dss);

    ctlM2Get("stats.arenas.0.dirty_decay_ms", i, &dirty_decay_ms);
    ctlM2Get("stats.arenas.0.muzzy_decay_ms", i, &muzzy_decay_ms);
    ctlM2Get("stats.arenas.0.pactive", i, &pactive);
    ctlM2Get("stats.arenas.0.pdirty", i, &pdirty);
    ctlM2Get("stats.arenas.0.pmuzzy", i, &pmuzzy);
    ctlM2Get("stats.arenas.0.dirty_npurge", i, &dirty_npurge);
    ctlM2Get("stats.arenas.0.dirty_nmadvise", i, &dirty_nmadvise);
    ctlM2Get("stats.arenas.0.dirty_purged", i, &dirty_purged);
    ctlM2Get("stats.arenas.0.muzzy_npurge", i, &muzzy_npurge);
    ctlM2Get("stats.arenas.0.muzzy_nmadvise", i, &muzzy_nmadvise);
    ctlM2Get("stats.arenas.0.muzzy_purged", i, &muzzy_purged);

    EmitterRow decay_row;
    decay_row.init();

    /// JSON-style emission.
    emitter.jsonKv("dirty_decay_ms", EmitterType::Ssize, &dirty_decay_ms);
    emitter.jsonKv("muzzy_decay_ms", EmitterType::Ssize, &muzzy_decay_ms);

    emitter.jsonKv("pactive", EmitterType::Size, &pactive);
    emitter.jsonKv("pdirty", EmitterType::Size, &pdirty);
    emitter.jsonKv("pmuzzy", EmitterType::Size, &pmuzzy);

    emitter.jsonKv("dirty_npurge", EmitterType::Uint64, &dirty_npurge);
    emitter.jsonKv("dirty_nmadvise", EmitterType::Uint64, &dirty_nmadvise);
    emitter.jsonKv("dirty_purged", EmitterType::Uint64, &dirty_purged);

    emitter.jsonKv("muzzy_npurge", EmitterType::Uint64, &muzzy_npurge);
    emitter.jsonKv("muzzy_nmadvise", EmitterType::Uint64, &muzzy_nmadvise);
    emitter.jsonKv("muzzy_purged", EmitterType::Uint64, &muzzy_purged);

    /// Table-style emission.
    EmitterCol col_decay_type;
    colInit(col_decay_type, decay_row, RIGHT, 9, EmitterType::Title);
    col_decay_type.str_val = "decaying:";

    EmitterCol col_decay_time;
    colInit(col_decay_time, decay_row, RIGHT, 6, EmitterType::Title);
    col_decay_time.str_val = "time";

    EmitterCol col_decay_npages;
    colInit(col_decay_npages, decay_row, RIGHT, 13, EmitterType::Title);
    col_decay_npages.str_val = "npages";

    EmitterCol col_decay_sweeps;
    colInit(col_decay_sweeps, decay_row, RIGHT, 13, EmitterType::Title);
    col_decay_sweeps.str_val = "sweeps";

    EmitterCol col_decay_madvises;
    colInit(col_decay_madvises, decay_row, RIGHT, 13, EmitterType::Title);
    col_decay_madvises.str_val = "madvises";

    EmitterCol col_decay_purged;
    colInit(col_decay_purged, decay_row, RIGHT, 13, EmitterType::Title);
    col_decay_purged.str_val = "purged";

    /// Title row.
    emitter.tableRow(decay_row);

    /// Dirty row.
    col_decay_type.str_val = "dirty:";

    if (dirty_decay_ms >= 0)
    {
        col_decay_time.type = EmitterType::Ssize;
        col_decay_time.ssize_val = dirty_decay_ms;
    }
    else
    {
        col_decay_time.type = EmitterType::Title;
        col_decay_time.str_val = "N/A";
    }

    col_decay_npages.type = EmitterType::Size;
    col_decay_npages.size_val = pdirty;

    col_decay_sweeps.type = EmitterType::Uint64;
    col_decay_sweeps.uint64_val = dirty_npurge;

    col_decay_madvises.type = EmitterType::Uint64;
    col_decay_madvises.uint64_val = dirty_nmadvise;

    col_decay_purged.type = EmitterType::Uint64;
    col_decay_purged.uint64_val = dirty_purged;

    emitter.tableRow(decay_row);

    /// Muzzy row.
    col_decay_type.str_val = "muzzy:";

    if (muzzy_decay_ms >= 0)
    {
        col_decay_time.type = EmitterType::Ssize;
        col_decay_time.ssize_val = muzzy_decay_ms;
    }
    else
    {
        col_decay_time.type = EmitterType::Title;
        col_decay_time.str_val = "N/A";
    }

    col_decay_npages.type = EmitterType::Size;
    col_decay_npages.size_val = pmuzzy;

    col_decay_sweeps.type = EmitterType::Uint64;
    col_decay_sweeps.uint64_val = muzzy_npurge;

    col_decay_madvises.type = EmitterType::Uint64;
    col_decay_madvises.uint64_val = muzzy_nmadvise;

    col_decay_purged.type = EmitterType::Uint64;
    col_decay_purged.uint64_val = muzzy_purged;

    emitter.tableRow(decay_row);

    /// Small / large / total allocation counts.
    EmitterRow alloc_count_row;
    alloc_count_row.init();

    EmitterCol col_count_title;
    colInit(col_count_title, alloc_count_row, LEFT, 21, EmitterType::Title);
    col_count_title.str_val = "";

    EmitterCol col_count_allocated;
    colInit(col_count_allocated, alloc_count_row, RIGHT, 16, EmitterType::Title);
    col_count_allocated.str_val = "allocated";

    EmitterCol col_count_nmalloc;
    colInit(col_count_nmalloc, alloc_count_row, RIGHT, 16, EmitterType::Title);
    col_count_nmalloc.str_val = "nmalloc";
    EmitterCol col_count_nmalloc_ps;
    colInit(col_count_nmalloc_ps, alloc_count_row, RIGHT, 10, EmitterType::Title);
    col_count_nmalloc_ps.str_val = "(#/sec)";

    EmitterCol col_count_ndalloc;
    colInit(col_count_ndalloc, alloc_count_row, RIGHT, 16, EmitterType::Title);
    col_count_ndalloc.str_val = "ndalloc";
    EmitterCol col_count_ndalloc_ps;
    colInit(col_count_ndalloc_ps, alloc_count_row, RIGHT, 10, EmitterType::Title);
    col_count_ndalloc_ps.str_val = "(#/sec)";

    EmitterCol col_count_nrequests;
    colInit(col_count_nrequests, alloc_count_row, RIGHT, 16, EmitterType::Title);
    col_count_nrequests.str_val = "nrequests";
    EmitterCol col_count_nrequests_ps;
    colInit(col_count_nrequests_ps, alloc_count_row, RIGHT, 10, EmitterType::Title);
    col_count_nrequests_ps.str_val = "(#/sec)";

    EmitterCol col_count_nfills;
    colInit(col_count_nfills, alloc_count_row, RIGHT, 16, EmitterType::Title);
    col_count_nfills.str_val = "nfill";
    EmitterCol col_count_nfills_ps;
    colInit(col_count_nfills_ps, alloc_count_row, RIGHT, 10, EmitterType::Title);
    col_count_nfills_ps.str_val = "(#/sec)";

    EmitterCol col_count_nflushes;
    colInit(col_count_nflushes, alloc_count_row, RIGHT, 16, EmitterType::Title);
    col_count_nflushes.str_val = "nflush";
    EmitterCol col_count_nflushes_ps;
    colInit(col_count_nflushes_ps, alloc_count_row, RIGHT, 10, EmitterType::Title);
    col_count_nflushes_ps.str_val = "(#/sec)";

    emitter.tableRow(alloc_count_row);

    col_count_nmalloc_ps.type = EmitterType::Uint64;
    col_count_ndalloc_ps.type = EmitterType::Uint64;
    col_count_nrequests_ps.type = EmitterType::Uint64;
    col_count_nfills_ps.type = EmitterType::Uint64;
    col_count_nflushes_ps.type = EmitterType::Uint64;

    /// The values of `small` and `large` (jemalloc: `small_allocated`, `small_nmalloc`, ..., `large_nflushes`).
    struct AllocStats
    {
        size_t allocated;
        uint64_t nmalloc;
        uint64_t ndalloc;
        uint64_t nrequests;
        uint64_t nfills;
        uint64_t nflushes;
    };
    AllocStats small_stats;
    AllocStats large_stats;

    /// jemalloc: GET_AND_EMIT_ALLOC_STAT
    auto get_and_emit_size = [&](const char * ctl_name, const char * json_name, size_t & var, EmitterCol & col)
    {
        ctlM2Get(ctl_name, i, &var);
        emitter.jsonKv(json_name, EmitterType::Size, &var);
        col.type = EmitterType::Size;
        col.size_val = var;
    };
    auto get_and_emit_uint64 = [&](const char * ctl_name, const char * json_name, uint64_t & var, EmitterCol & col)
    {
        ctlM2Get(ctl_name, i, &var);
        emitter.jsonKv(json_name, EmitterType::Uint64, &var);
        col.type = EmitterType::Uint64;
        col.uint64_val = var;
    };

    emitter.jsonObjectKvBegin("small");
    col_count_title.str_val = "small:";

    get_and_emit_size("stats.arenas.0.small.allocated", "allocated", small_stats.allocated, col_count_allocated);
    get_and_emit_uint64("stats.arenas.0.small.nmalloc", "nmalloc", small_stats.nmalloc, col_count_nmalloc);
    col_count_nmalloc_ps.uint64_val = ratePerSecond(col_count_nmalloc.uint64_val, uptime);
    get_and_emit_uint64("stats.arenas.0.small.ndalloc", "ndalloc", small_stats.ndalloc, col_count_ndalloc);
    col_count_ndalloc_ps.uint64_val = ratePerSecond(col_count_ndalloc.uint64_val, uptime);
    get_and_emit_uint64("stats.arenas.0.small.nrequests", "nrequests", small_stats.nrequests, col_count_nrequests);
    col_count_nrequests_ps.uint64_val = ratePerSecond(col_count_nrequests.uint64_val, uptime);
    get_and_emit_uint64("stats.arenas.0.small.nfills", "nfills", small_stats.nfills, col_count_nfills);
    col_count_nfills_ps.uint64_val = ratePerSecond(col_count_nfills.uint64_val, uptime);
    get_and_emit_uint64("stats.arenas.0.small.nflushes", "nflushes", small_stats.nflushes, col_count_nflushes);
    col_count_nflushes_ps.uint64_val = ratePerSecond(col_count_nflushes.uint64_val, uptime);

    emitter.tableRow(alloc_count_row);
    emitter.jsonObjectEnd(); /// Close "small".

    emitter.jsonObjectKvBegin("large");
    col_count_title.str_val = "large:";

    get_and_emit_size("stats.arenas.0.large.allocated", "allocated", large_stats.allocated, col_count_allocated);
    get_and_emit_uint64("stats.arenas.0.large.nmalloc", "nmalloc", large_stats.nmalloc, col_count_nmalloc);
    col_count_nmalloc_ps.uint64_val = ratePerSecond(col_count_nmalloc.uint64_val, uptime);
    get_and_emit_uint64("stats.arenas.0.large.ndalloc", "ndalloc", large_stats.ndalloc, col_count_ndalloc);
    col_count_ndalloc_ps.uint64_val = ratePerSecond(col_count_ndalloc.uint64_val, uptime);
    get_and_emit_uint64("stats.arenas.0.large.nrequests", "nrequests", large_stats.nrequests, col_count_nrequests);
    col_count_nrequests_ps.uint64_val = ratePerSecond(col_count_nrequests.uint64_val, uptime);
    get_and_emit_uint64("stats.arenas.0.large.nfills", "nfills", large_stats.nfills, col_count_nfills);
    col_count_nfills_ps.uint64_val = ratePerSecond(col_count_nfills.uint64_val, uptime);
    get_and_emit_uint64("stats.arenas.0.large.nflushes", "nflushes", large_stats.nflushes, col_count_nflushes);
    col_count_nflushes_ps.uint64_val = ratePerSecond(col_count_nflushes.uint64_val, uptime);

    emitter.tableRow(alloc_count_row);
    emitter.jsonObjectEnd(); /// Close "large".

    /// Aggregated small + large stats are emitter only in table mode.
    col_count_title.str_val = "total:";
    col_count_allocated.size_val = small_stats.allocated + large_stats.allocated;
    col_count_nmalloc.uint64_val = small_stats.nmalloc + large_stats.nmalloc;
    col_count_ndalloc.uint64_val = small_stats.ndalloc + large_stats.ndalloc;
    col_count_nrequests.uint64_val = small_stats.nrequests + large_stats.nrequests;
    col_count_nfills.uint64_val = small_stats.nfills + large_stats.nfills;
    col_count_nflushes.uint64_val = small_stats.nflushes + large_stats.nflushes;
    col_count_nmalloc_ps.uint64_val = ratePerSecond(col_count_nmalloc.uint64_val, uptime);
    col_count_ndalloc_ps.uint64_val = ratePerSecond(col_count_ndalloc.uint64_val, uptime);
    col_count_nrequests_ps.uint64_val = ratePerSecond(col_count_nrequests.uint64_val, uptime);
    col_count_nfills_ps.uint64_val = ratePerSecond(col_count_nfills.uint64_val, uptime);
    col_count_nflushes_ps.uint64_val = ratePerSecond(col_count_nflushes.uint64_val, uptime);
    emitter.tableRow(alloc_count_row);

    EmitterRow mem_count_row;
    mem_count_row.init();

    EmitterCol mem_count_title;
    mem_count_title.init(mem_count_row);
    mem_count_title.justify = LEFT;
    mem_count_title.width = 21;
    mem_count_title.type = EmitterType::Title;
    mem_count_title.str_val = "";

    EmitterCol mem_count_val;
    mem_count_val.init(mem_count_row);
    mem_count_val.justify = RIGHT;
    mem_count_val.width = 16;
    mem_count_val.type = EmitterType::Title;
    mem_count_val.str_val = "";

    emitter.tableRow(mem_count_row);
    mem_count_val.type = EmitterType::Size;

    /// Active count in bytes is emitted only in table mode.
    mem_count_title.str_val = "active:";
    mem_count_val.size_val = pactive * page;
    emitter.tableRow(mem_count_row);

    /// jemalloc: GET_AND_EMIT_MEM_STAT
    struct MemStat
    {
        const char * ctl_name;
        const char * json_name;
        const char * table_name;
    };
    static constexpr MemStat mem_stats[] = {
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
    for (const MemStat & stat : mem_stats)
    {
        size_t value;
        ctlM2Get(stat.ctl_name, i, &value);
        emitter.jsonKv(stat.json_name, EmitterType::Size, &value);
        mem_count_title.str_val = stat.table_name;
        mem_count_val.size_val = value;
        emitter.tableRow(mem_count_row);
    }

    if (mutex)
        statsArenaMutexesPrint(emitter, i, uptime);
    if (bins)
        statsArenaBinsPrint(emitter, mutex, i, uptime);
    if (large)
        statsArenaLextentsPrint(emitter, i, uptime);
    if (extents)
        statsArenaExtentsPrint(emitter, i);
    if (hpa)
        statsArenaHpaShardPrint(emitter, i, uptime);
}

/// --- General information ----------------------------------------------------------------------------------------

/// The local variables of `stats_general_print` shared by the `OPT_WRITE_*` macros.
struct GeneralPrintVars
{
    const char * cpv;
    bool bv;
    bool bv2;
    unsigned uv;
    uint64_t u64v;
    int64_t i64v;
    ssize_t ssv;
    ssize_t ssv2;
    size_t sv;
    size_t bsz = sizeof(bool);
    size_t usz = sizeof(unsigned);
    size_t ssz = sizeof(size_t);
    size_t sssz = sizeof(ssize_t);
    size_t cpsz = sizeof(const char *);
    size_t i64sz = sizeof(int64_t);
    size_t u64sz = sizeof(uint64_t);
};

/// Prints `opt.<name>` only if the mallctl succeeds. `size` is in/out like in jemalloc (it is shared between calls).
/// jemalloc: OPT_WRITE
template <typename T>
void optWrite(Emitter & emitter, const char * json_name, const char * table_name, T & var, size_t & size, EmitterType type)
{
    if (mallctl(table_name, static_cast<void *>(&var), &size, nullptr, 0) == 0)
        emitter.kv(json_name, table_name, type, &var);
}

/// jemalloc: OPT_WRITE_MUTABLE
template <typename T>
void optWriteMutable(
    Emitter & emitter, const char * json_name, const char * table_name, T & var1, T & var2, size_t & size, EmitterType type, const char * altname)
{
    if (mallctl(table_name, static_cast<void *>(&var1), &size, nullptr, 0) == 0
        && mallctl(altname, static_cast<void *>(&var2), &size, nullptr, 0) == 0)
        emitter.kvNote(json_name, table_name, type, &var1, altname, type, &var2);
}

/// jemalloc: stats_general_print
JE_COLD void statsGeneralPrint(Emitter & emitter)
{
    GeneralPrintVars v;
    uint32_t u32v;
    size_t u32sz = sizeof(uint32_t);

    ctlGet("version", &v.cpv);
    emitter.kv("version", "Version", EmitterType::String, &v.cpv);

    /// config.
    emitter.dictBegin("config", "Build-time option settings");

    /// jemalloc: CONFIG_WRITE_BOOL
    auto config_write_bool = [&](const char * json_name, const char * table_name)
    {
        ctlGet(table_name, &v.bv);
        emitter.kv(json_name, table_name, EmitterType::Bool, &v.bv);
    };

    config_write_bool("cache_oblivious", "config.cache_oblivious");
    config_write_bool("debug", "config.debug");
    config_write_bool("fill", "config.fill");
    config_write_bool("lazy_lock", "config.lazy_lock");
    const char * config_malloc_conf = config::malloc_conf_default;
    emitter.kv("malloc_conf", "config.malloc_conf", EmitterType::String, &config_malloc_conf);

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
    const char * thp_mode_name = system_thp_mode_names[static_cast<unsigned>(init_system_thp_mode)];
    emitter.kv("thp_mode", "system.thp_mode", EmitterType::String, &thp_mode_name);

    emitter.dictEnd(); /// Close "system".

    /// opt.
#define OPT_WRITE_BOOL(name) optWrite(emitter, name, "opt." name, v.bv, v.bsz, EmitterType::Bool);
#define OPT_WRITE_BOOL_MUTABLE(name, altname) \
    optWriteMutable(emitter, name, "opt." name, v.bv, v.bv2, v.bsz, EmitterType::Bool, altname);
#define OPT_WRITE_UNSIGNED(name) optWrite(emitter, name, "opt." name, v.uv, v.usz, EmitterType::Unsigned);
#define OPT_WRITE_INT64(name) optWrite(emitter, name, "opt." name, v.i64v, v.i64sz, EmitterType::Int64);
#define OPT_WRITE_UINT64(name) optWrite(emitter, name, "opt." name, v.u64v, v.u64sz, EmitterType::Uint64);
#define OPT_WRITE_SIZE_T(name) optWrite(emitter, name, "opt." name, v.sv, v.ssz, EmitterType::Size);
#define OPT_WRITE_SSIZE_T(name) optWrite(emitter, name, "opt." name, v.ssv, v.sssz, EmitterType::Ssize);
#define OPT_WRITE_SSIZE_T_MUTABLE(name, altname) \
    optWriteMutable(emitter, name, "opt." name, v.ssv, v.ssv2, v.sssz, EmitterType::Ssize, altname);
#define OPT_WRITE_CHAR_P(name) optWrite(emitter, name, "opt." name, v.cpv, v.cpsz, EmitterType::String);

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
    auto malloc_conf_write = [&](const char * ctl_name, const char * json_name, const char * message)
    {
        if (mallctl(ctl_name, static_cast<void *>(&v.cpv), &v.cpsz, nullptr, 0) != 0)
            v.cpv = "";
        emitter.kv(json_name, message, EmitterType::String, &v.cpv);
    };

    malloc_conf_write("opt.malloc_conf.global_var", "global_var", "Global variable malloc_conf");
    malloc_conf_write("opt.malloc_conf.symlink", "symlink", "Symbolic link malloc.conf");
    malloc_conf_write("opt.malloc_conf.env_var", "env_var", "Environment variable MALLOC_CONF");
    /// As this config is unofficial, skip the output if it's NULL
    if (mallctl("opt.malloc_conf.global_var_2_conf_harder", static_cast<void *>(&v.cpv), &v.cpsz, nullptr, 0) == 0)
        emitter.kv("global_var_2_conf_harder", "Global variable malloc_conf_2_conf_harder", EmitterType::String, &v.cpv);

    OPT_WRITE_BOOL("abort")
    OPT_WRITE_BOOL("abort_conf")
    OPT_WRITE_BOOL("cache_oblivious")
    OPT_WRITE_BOOL("confirm_conf")
    OPT_WRITE_BOOL("experimental_hpa_start_huge_if_thp_always")
    OPT_WRITE_BOOL("experimental_hpa_enforce_hugify")
    OPT_WRITE_BOOL("retain")
    OPT_WRITE_CHAR_P("dss")
    OPT_WRITE_UNSIGNED("narenas")
    OPT_WRITE_CHAR_P("percpu_arena")
    OPT_WRITE_SIZE_T("oversize_threshold")
    OPT_WRITE_BOOL("hpa")
    OPT_WRITE_SIZE_T("hpa_slab_max_alloc")
    OPT_WRITE_SIZE_T("hpa_hugification_threshold")
    OPT_WRITE_UINT64("hpa_hugify_delay_ms")
    OPT_WRITE_BOOL("hpa_hugify_sync")
    OPT_WRITE_UINT64("hpa_min_purge_interval_ms")
    OPT_WRITE_SSIZE_T("experimental_hpa_max_purge_nhp")
    if (mallctl("opt.hpa_dirty_mult", static_cast<void *>(&u32v), &u32sz, nullptr, 0) == 0)
    {
        /// We cheat a little and "know" the secret meaning of this representation.
        if (u32v == static_cast<uint32_t>(-1))
        {
            const char * neg1 = "-1";
            emitter.kv("hpa_dirty_mult", "opt.hpa_dirty_mult", EmitterType::String, &neg1);
        }
        else
        {
            char buf[fxp::BUF_SIZE];
            fxp::print(u32v, buf);
            const char * bufp = buf;
            emitter.kv("hpa_dirty_mult", "opt.hpa_dirty_mult", EmitterType::String, &bufp);
        }
    }
    OPT_WRITE_SIZE_T("hpa_purge_threshold")
    OPT_WRITE_UINT64("hpa_min_purge_delay_ms")
    OPT_WRITE_CHAR_P("hpa_hugify_style")
    OPT_WRITE_SIZE_T("hpa_sec_nshards")
    OPT_WRITE_SIZE_T("hpa_sec_max_alloc")
    OPT_WRITE_SIZE_T("hpa_sec_max_bytes")
    OPT_WRITE_SIZE_T("hpa_sec_batch_fill_extra")
    OPT_WRITE_BOOL("huge_arena_pac_thp")
    OPT_WRITE_CHAR_P("metadata_thp")
    OPT_WRITE_INT64("mutex_max_spin")
    OPT_WRITE_BOOL_MUTABLE("background_thread", "background_thread")
    OPT_WRITE_SSIZE_T_MUTABLE("dirty_decay_ms", "arenas.dirty_decay_ms")
    OPT_WRITE_SSIZE_T_MUTABLE("muzzy_decay_ms", "arenas.muzzy_decay_ms")
    OPT_WRITE_SIZE_T("lg_extent_max_active_fit")
    OPT_WRITE_CHAR_P("junk")
    OPT_WRITE_BOOL("zero")
    OPT_WRITE_BOOL("utrace")
    OPT_WRITE_BOOL("xmalloc")
    OPT_WRITE_BOOL("experimental_infallible_new")
    OPT_WRITE_BOOL("experimental_tcache_gc")
    OPT_WRITE_BOOL("tcache")
    OPT_WRITE_SIZE_T("tcache_max")
    OPT_WRITE_UNSIGNED("tcache_nslots_small_min")
    OPT_WRITE_UNSIGNED("tcache_nslots_small_max")
    OPT_WRITE_UNSIGNED("tcache_nslots_large")
    OPT_WRITE_SSIZE_T("lg_tcache_nslots_mul")
    OPT_WRITE_SIZE_T("tcache_gc_incr_bytes")
    OPT_WRITE_SIZE_T("tcache_gc_delay_bytes")
    OPT_WRITE_UNSIGNED("lg_tcache_flush_small_div")
    OPT_WRITE_UNSIGNED("lg_tcache_flush_large_div")
    OPT_WRITE_UNSIGNED("debug_double_free_max_scan")
    OPT_WRITE_CHAR_P("thp")
    OPT_WRITE_BOOL("prof")
    OPT_WRITE_UNSIGNED("prof_bt_max")
    OPT_WRITE_CHAR_P("prof_prefix")
    OPT_WRITE_BOOL_MUTABLE("prof_active", "prof.active")
    OPT_WRITE_BOOL_MUTABLE("prof_thread_active_init", "prof.thread_active_init")
    OPT_WRITE_SSIZE_T_MUTABLE("lg_prof_sample", "prof.lg_sample")
    OPT_WRITE_BOOL("prof_accum")
    OPT_WRITE_SSIZE_T("lg_prof_interval")
    OPT_WRITE_BOOL("prof_gdump")
    OPT_WRITE_BOOL("prof_final")
    OPT_WRITE_BOOL("prof_leak")
    OPT_WRITE_BOOL("prof_leak_error")
    /// jemalloc compatibility: `stats_print` and `stats_print_opts` are printed twice.
    OPT_WRITE_BOOL("stats_print")
    OPT_WRITE_CHAR_P("stats_print_opts")
    OPT_WRITE_BOOL("stats_print")
    OPT_WRITE_CHAR_P("stats_print_opts")
    OPT_WRITE_INT64("stats_interval")
    OPT_WRITE_CHAR_P("stats_interval_opts")
    OPT_WRITE_CHAR_P("zero_realloc")
    OPT_WRITE_SIZE_T("process_madvise_max_batch")
    OPT_WRITE_BOOL("disable_large_size_classes")

    emitter.dictEnd(); /// Close "opt".

#undef OPT_WRITE_BOOL
#undef OPT_WRITE_BOOL_MUTABLE
#undef OPT_WRITE_UNSIGNED
#undef OPT_WRITE_INT64
#undef OPT_WRITE_UINT64
#undef OPT_WRITE_SIZE_T
#undef OPT_WRITE_SSIZE_T
#undef OPT_WRITE_SSIZE_T_MUTABLE
#undef OPT_WRITE_CHAR_P

    /// prof.
    if constexpr (config::prof)
    {
        emitter.dictBegin("prof", "Profiling settings");

        ctlGet("prof.thread_active_init", &v.bv);
        emitter.kv("thread_active_init", "prof.thread_active_init", EmitterType::Bool, &v.bv);

        ctlGet("prof.active", &v.bv);
        emitter.kv("active", "prof.active", EmitterType::Bool, &v.bv);

        ctlGet("prof.gdump", &v.bv);
        emitter.kv("gdump", "prof.gdump", EmitterType::Bool, &v.bv);

        ctlGet("prof.interval", &v.u64v);
        emitter.kv("interval", "prof.interval", EmitterType::Uint64, &v.u64v);

        ctlGet("prof.lg_sample", &v.ssv);
        emitter.kv("lg_sample", "prof.lg_sample", EmitterType::Ssize, &v.ssv);

        emitter.dictEnd(); /// Close "prof".
    }

    /// arenas.
    /// The json output sticks arena info into an "arenas" dict; the table output puts them at the top-level.
    emitter.jsonObjectKvBegin("arenas");

    ctlGet("arenas.narenas", &v.uv);
    emitter.kv("narenas", "Arenas", EmitterType::Unsigned, &v.uv);

    /// Decay settings are emitted only in json mode; in table mode, they're emitted as notes with the opt output,
    /// above.
    ctlGet("arenas.dirty_decay_ms", &v.ssv);
    emitter.jsonKv("dirty_decay_ms", EmitterType::Ssize, &v.ssv);

    ctlGet("arenas.muzzy_decay_ms", &v.ssv);
    emitter.jsonKv("muzzy_decay_ms", EmitterType::Ssize, &v.ssv);

    ctlGet("arenas.quantum", &v.sv);
    emitter.kv("quantum", "Quantum size", EmitterType::Size, &v.sv);

    ctlGet("arenas.page", &v.sv);
    emitter.kv("page", "Page size", EmitterType::Size, &v.sv);

    ctlGet("arenas.hugepage", &v.sv);
    emitter.kv("hugepage", "Hugepage size", EmitterType::Size, &v.sv);

    if (mallctl("arenas.tcache_max", static_cast<void *>(&v.sv), &v.ssz, nullptr, 0) == 0)
        emitter.kv("tcache_max", "Maximum thread-cached size class", EmitterType::Size, &v.sv);

    unsigned arenas_nbins;
    ctlGet("arenas.nbins", &arenas_nbins);
    emitter.kv("nbins", "Number of bin size classes", EmitterType::Unsigned, &arenas_nbins);

    unsigned arenas_nhbins;
    ctlGet("arenas.nhbins", &arenas_nhbins);
    emitter.kv("nhbins", "Number of thread-cache bin size classes", EmitterType::Unsigned, &arenas_nhbins);

    /// We do enough mallctls in a loop that we actually want to omit them (not just omit the printing).
    if (emitter.outputsJSON())
    {
        emitter.jsonArrayKvBegin("bin");
        size_t arenas_bin_mib[CTL_MAX_DEPTH];
        ctlLeafPrepare(arenas_bin_mib, 0, "arenas.bin");
        for (unsigned i = 0; i < arenas_nbins; i++)
        {
            arenas_bin_mib[2] = i;
            emitter.jsonObjectBegin();

            ctlLeaf(arenas_bin_mib, 3, "size", &v.sv);
            emitter.jsonKv("size", EmitterType::Size, &v.sv);

            ctlLeaf(arenas_bin_mib, 3, "nregs", &u32v);
            emitter.jsonKv("nregs", EmitterType::Uint32, &u32v);

            ctlLeaf(arenas_bin_mib, 3, "slab_size", &v.sv);
            emitter.jsonKv("slab_size", EmitterType::Size, &v.sv);

            ctlLeaf(arenas_bin_mib, 3, "nshards", &u32v);
            emitter.jsonKv("nshards", EmitterType::Uint32, &u32v);

            emitter.jsonObjectEnd();
        }
        emitter.jsonArrayEnd(); /// Close "bin".
    }

    unsigned nlextents;
    ctlGet("arenas.nlextents", &nlextents);
    emitter.kv("nlextents", "Number of large size classes", EmitterType::Unsigned, &nlextents);

    if (emitter.outputsJSON())
    {
        emitter.jsonArrayKvBegin("lextent");
        size_t arenas_lextent_mib[CTL_MAX_DEPTH];
        ctlLeafPrepare(arenas_lextent_mib, 0, "arenas.lextent");
        for (unsigned i = 0; i < nlextents; i++)
        {
            arenas_lextent_mib[2] = i;
            emitter.jsonObjectBegin();

            ctlLeaf(arenas_lextent_mib, 3, "size", &v.sv);
            emitter.jsonKv("size", EmitterType::Size, &v.sv);

            emitter.jsonObjectEnd();
        }
        emitter.jsonArrayEnd(); /// Close "lextent".
    }

    emitter.jsonObjectEnd(); /// Close "arenas"
}

/// jemalloc: stats_print_helper
JE_COLD void statsPrintHelper(
    Emitter & emitter, bool merged, bool destroyed, bool unmerged, bool bins, bool large, bool mutex, bool extents, bool hpa)
{
    /// These should be deleted. We keep them around for a while, to aid in the transition to the emitter code.
    size_t allocated;
    size_t active;
    size_t metadata;
    size_t metadata_edata;
    size_t metadata_rtree;
    size_t metadata_thp;
    size_t resident;
    size_t mapped;
    size_t retained;
    size_t num_background_threads;
    size_t zero_reallocs;
    uint64_t background_thread_num_runs;
    uint64_t background_thread_run_interval;

    ctlGet("stats.allocated", &allocated);
    ctlGet("stats.active", &active);
    ctlGet("stats.metadata", &metadata);
    ctlGet("stats.metadata_edata", &metadata_edata);
    ctlGet("stats.metadata_rtree", &metadata_rtree);
    ctlGet("stats.metadata_thp", &metadata_thp);
    ctlGet("stats.resident", &resident);
    ctlGet("stats.mapped", &mapped);
    ctlGet("stats.retained", &retained);

    ctlGet("stats.zero_reallocs", &zero_reallocs);

    if constexpr (config::background_thread)
    {
        ctlGet("stats.background_thread.num_threads", &num_background_threads);
        ctlGet("stats.background_thread.num_runs", &background_thread_num_runs);
        ctlGet("stats.background_thread.run_interval", &background_thread_run_interval);
    }
    else
    {
        num_background_threads = 0;
        background_thread_num_runs = 0;
        background_thread_run_interval = 0;
    }

    /// Generic global stats.
    emitter.jsonObjectKvBegin("stats");
    emitter.jsonKv("allocated", EmitterType::Size, &allocated);
    emitter.jsonKv("active", EmitterType::Size, &active);
    emitter.jsonKv("metadata", EmitterType::Size, &metadata);
    emitter.jsonKv("metadata_edata", EmitterType::Size, &metadata_edata);
    emitter.jsonKv("metadata_rtree", EmitterType::Size, &metadata_rtree);
    emitter.jsonKv("metadata_thp", EmitterType::Size, &metadata_thp);
    emitter.jsonKv("resident", EmitterType::Size, &resident);
    emitter.jsonKv("mapped", EmitterType::Size, &mapped);
    emitter.jsonKv("retained", EmitterType::Size, &retained);
    emitter.jsonKv("zero_reallocs", EmitterType::Size, &zero_reallocs);

    emitter.tablePrintf(
        "Allocated: %zu, active: %zu, "
        "metadata: %zu (n_thp %zu, edata %zu, rtree %zu), resident: %zu, "
        "mapped: %zu, retained: %zu\n",
        allocated,
        active,
        metadata,
        metadata_thp,
        metadata_edata,
        metadata_rtree,
        resident,
        mapped,
        retained);

    /// Strange behaviors
    emitter.tablePrintf("Count of realloc(non-null-ptr, 0) calls: %zu\n", zero_reallocs);

    /// Background thread stats.
    emitter.jsonObjectKvBegin("background_thread");
    emitter.jsonKv("num_threads", EmitterType::Size, &num_background_threads);
    emitter.jsonKv("num_runs", EmitterType::Uint64, &background_thread_num_runs);
    emitter.jsonKv("run_interval", EmitterType::Uint64, &background_thread_run_interval);
    emitter.jsonObjectEnd(); /// Close "background_thread".

    emitter.tablePrintf(
        "Background threads: %zu, "
        "num_runs: %" FMTu64 ", run_interval: %" FMTu64 " ns\n",
        num_background_threads,
        background_thread_num_runs,
        background_thread_run_interval);

    if (mutex)
    {
        EmitterRow row;
        EmitterCol name;
        MutexCols64 col64;
        MutexCols32 col32;
        uint64_t uptime;

        row.init();
        mutexStatsInitCols(row, "", &name, col64, col32);

        emitter.tableRow(row);
        emitter.jsonObjectKvBegin("mutexes");

        ctlM2Get("stats.arenas.0.uptime", 0, &uptime);

        size_t stats_mutexes_mib[CTL_MAX_DEPTH];
        ctlLeafPrepare(stats_mutexes_mib, 0, "stats.mutexes");
        for (unsigned i = 0; i < mutex_prof_num_global_mutexes; i++)
        {
            mutexStatsReadNamed(stats_mutexes_mib, 2, mutex_prof_global_names[i], &name, col64, col32, uptime);
            emitter.jsonObjectKvBegin(mutex_prof_global_names[i]);
            mutexStatsEmit(emitter, &row, col64, col32);
            emitter.jsonObjectEnd();
        }

        emitter.jsonObjectEnd(); /// Close "mutexes".
    }

    emitter.jsonObjectEnd(); /// Close "stats".

    if (merged || destroyed || unmerged)
    {
        unsigned narenas;

        emitter.jsonObjectKvBegin("stats.arenas");

        ctlGet("arenas.narenas", &narenas);
        size_t mib[3];
        size_t miblen = sizeof(mib) / sizeof(size_t);
        size_t sz;
        /// jemalloc: VARIABLE_ARRAY_UNSAFE (a stack array)
        bool * initialized = static_cast<bool *>(__builtin_alloca(narenas * sizeof(bool)));
        bool destroyed_initialized;
        unsigned i;
        unsigned ninitialized;

        xmallctlNameToMib("arena.0.initialized", mib, &miblen);
        for (i = ninitialized = 0; i < narenas; i++)
        {
            mib[1] = i;
            sz = sizeof(bool);
            xmallctlByMib(mib, miblen, &initialized[i], &sz, nullptr, 0);
            if (initialized[i])
                ninitialized++;
        }
        mib[1] = MALLCTL_ARENAS_DESTROYED;
        sz = sizeof(bool);
        xmallctlByMib(mib, miblen, &destroyed_initialized, &sz, nullptr, 0);

        /// Merged stats.
        if (merged && (ninitialized > 1 || !unmerged))
        {
            /// Print merged arena stats.
            emitter.tablePrintf("Merged arenas stats:\n");
            emitter.jsonObjectKvBegin("merged");
            statsArenaPrint(emitter, MALLCTL_ARENAS_ALL, bins, large, mutex, extents, hpa);
            emitter.jsonObjectEnd(); /// Close "merged".
        }

        /// Destroyed stats.
        if (destroyed_initialized && destroyed)
        {
            /// Print destroyed arena stats.
            emitter.tablePrintf("Destroyed arenas stats:\n");
            emitter.jsonObjectKvBegin("destroyed");
            statsArenaPrint(emitter, MALLCTL_ARENAS_DESTROYED, bins, large, mutex, extents, hpa);
            emitter.jsonObjectEnd(); /// Close "destroyed".
        }

        /// Unmerged stats.
        if (unmerged)
        {
            for (i = 0; i < narenas; i++)
            {
                if (initialized[i])
                {
                    char arena_ind_str[20];
                    format(arena_ind_str, sizeof(arena_ind_str), "%u", i);
                    emitter.jsonObjectKvBegin(arena_ind_str);
                    emitter.tablePrintf("arenas[%s]:\n", arena_ind_str);
                    statsArenaPrint(emitter, i, bins, large, mutex, extents, hpa);
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
void statsPrint(WriteCallback * write_cb, void * cbopaque, const char * opts)
{
    int err;
    uint64_t epoch;
    size_t u64sz;
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
    bool hpa = config::stats;

    /// Refresh stats, in case mallctl() was called by the application.
    ///
    /// Check for OOM here, since refreshing the ctl cache can trigger allocation. In practice, none of the subsequent
    /// mallctl()-related calls in this function will cause OOM if this one succeeds.
    epoch = 1;
    u64sz = sizeof(uint64_t);
    err = mallctl("epoch", static_cast<void *>(&epoch), &u64sz, static_cast<void *>(&epoch), sizeof(uint64_t));
    if (err != 0)
    {
        if (err == EAGAIN)
        {
            writeMessage("<jemalloc>: Memory allocation failure in mallctl(\"epoch\", ...)\n");
            return;
        }
        writeMessage("<jemalloc>: Failure in mallctl(\"epoch\", ...)\n");
        abort();
    }

    if (opts != nullptr)
    {
        for (unsigned i = 0; opts[i] != '\0'; i++)
        {
            switch (opts[i])
            {
                case 'J':
                    json = true;
                    break;
                case 'g':
                    general = false;
                    break;
                case 'm':
                    merged = false;
                    break;
                case 'd':
                    destroyed = false;
                    break;
                case 'a':
                    unmerged = false;
                    break;
                case 'b':
                    bins = false;
                    break;
                case 'l':
                    large = false;
                    break;
                case 'x':
                    mutex = false;
                    break;
                case 'e':
                    extents = false;
                    break;
                case 'h':
                    hpa = false;
                    break;
                default:;
            }
        }
    }

    Emitter emitter(json ? EmitterOutput::JSONCompact : EmitterOutput::Table, write_cb, cbopaque);
    emitter.begin();
    emitter.tablePrintf("___ Begin jemalloc statistics ___\n");
    emitter.jsonObjectKvBegin("jemalloc");

    if (general)
        statsGeneralPrint(emitter);
    if constexpr (config::stats)
        statsPrintHelper(emitter, merged, destroyed, unmerged, bins, large, mutex, extents, hpa);

    emitter.jsonObjectEnd(); /// Closes the "jemalloc" dict.
    emitter.tablePrintf("--- End jemalloc statistics ---\n");
    emitter.end();
}

}
