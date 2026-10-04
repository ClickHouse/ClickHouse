#pragma once

/// The OS interface for mapping, committing, purging and advising pages, and the mmap-based extent allocation.
/// jemalloc: `pages.h`, `src/pages.c`, `extent_mmap.h`, `src/extent_mmap.c`.
///
/// All functions returning `bool` return true on error (as in jemalloc).
///
/// Platform differences (selected with `if constexpr` on `config::*`, or `#ifdef` where a system API does not exist):
/// - Linux: `MAP_NORESERVE` when the kernel overcommits (`/proc/sys/vm/overcommit_memory` is 0 or 1), VMA naming with
///   `prctl(PR_SET_VMA)` (except s390x), forced purge with `MADV_DONTNEED` (after a boot-time probe that it zeroes),
///   THP mode from sysfs, `MADV_HUGEPAGE`/`MADV_NOHUGEPAGE`, `MADV_DONTDUMP`/`MADV_DODUMP`.
/// - FreeBSD: overcommit via `sysctl vm.overcommit`, `pages::map` uses `MAP_FIXED | MAP_EXCL` or `MAP_ALIGNED`,
///   forced purge by overlaying a fresh mapping (`MAP_FIXED`), `MADV_NOCORE`/`MADV_CORE`, no THP.
/// - Darwin: never overcommits, mmap with the `VM_MAKE_TAG(254)` fd tag, forced purge by remapping, no THP.

#include <allocator/Common.h>
#include <allocator/Options.h>

#include <cstddef>

namespace jemalloc
{

/// Values of the `thp` option (`opt_thp`).
/// jemalloc: thp_mode_t
enum class ThpMode : unsigned
{
    /// Respect kernel THP settings. jemalloc: thp_mode_do_nothing
    DoNothing = 0,
    /// Always set `MADV_HUGEPAGE`. jemalloc: thp_mode_always
    Always = 1,
    /// Always set `MADV_NOHUGEPAGE`. jemalloc: thp_mode_never
    Never = 2,
    /// No THP support detected. jemalloc: thp_mode_not_supported
    NotSupported = 3,
};

/// The number of values accepted by the `thp` option. jemalloc: thp_mode_names_limit
inline constexpr unsigned thp_mode_names_limit = 3;

/// The kernel THP setting detected at boot (`init_system_thp_mode`).
/// jemalloc: system_thp_mode_t
enum class SystemThpMode : unsigned
{
    Madvise = 0,
    Always = 1,
    Never = 2,
    NotSupported = 3,
};

/// jemalloc: THP_MODE_DEFAULT
inline constexpr ThpMode THP_MODE_DEFAULT = ThpMode::DoNothing;

/// jemalloc: thp_mode_names
extern const char * const thp_mode_names[];
/// jemalloc: system_thp_mode_names
extern const char * const system_thp_mode_names[];

/// Values of the `metadata_thp` option.
/// jemalloc: metadata_thp_mode_t (`base.h`)
enum class MetadataThpMode : unsigned
{
    Disabled = 0,
    /// Lazily enable hugepage for metadata. To avoid high RSS caused by THP + low usage arena (i.e. THP becomes a
    /// significant percentage), the "auto" option only starts using THP after a base allocator used up the first THP
    /// region. Starting from the second hugepage (in a single arena), "auto" behaves the same as "always", i.e.
    /// madvise hugepage right away.
    Auto = 1,
    Always = 2,
};

/// jemalloc: metadata_thp_mode_limit
inline constexpr unsigned metadata_thp_mode_limit = 3;

/// jemalloc: METADATA_THP_DEFAULT
inline constexpr MetadataThpMode METADATA_THP_DEFAULT = MetadataThpMode::Disabled;

/// jemalloc: metadata_thp_mode_names (`src/base.c`)
extern const char * const metadata_thp_mode_names[];

/// jemalloc: metadata_thp_enabled
inline bool metadataThpEnabled()
{
    return opt.metadata_thp != MetadataThpMode::Disabled;
}

/// Initial system-wide THP state.
/// jemalloc: init_system_thp_mode
extern SystemThpMode init_system_thp_mode;

/// Actual operating system page size, detected during bootstrap, <= PAGE.
/// jemalloc: os_page
extern size_t os_page;

namespace pages
{

/// jemalloc: pages_can_purge_lazy (`PAGES_CAN_PURGE_LAZY`: `JEMALLOC_PURGE_MADVISE_FREE`, all supported platforms).
inline constexpr bool can_purge_lazy = true;
/// jemalloc: pages_can_purge_forced (`PAGES_CAN_PURGE_FORCED`: DONTNEED with zeroing, or `JEMALLOC_MAPS_COALESCE`).
inline constexpr bool can_purge_forced = config::purge_madvise_dontneed_zeros || config::maps_coalesce;
/// jemalloc: pages_can_hugify (`PAGES_CAN_HUGIFY`: `JEMALLOC_HAVE_MADVISE_HUGE`).
inline constexpr bool can_hugify = config::have_madvise_huge;

/// Map `size` bytes aligned to `alignment` (>= PAGE). `addr` is a hint: if non-null, the mapping must be exactly
/// there or the call fails (existing mappings are never replaced). `*commit` is set to true when the OS overcommits.
/// Returns nullptr on failure.
/// jemalloc: pages_map
void * map(void * addr, size_t size, size_t alignment, bool * commit);

/// jemalloc: pages_unmap
void unmap(void * addr, size_t size);

/// Returns true on error (always when the OS overcommits).
/// jemalloc: pages_commit
bool commit(void * addr, size_t size);

/// Returns true on error (always when the OS overcommits).
/// jemalloc: pages_decommit
bool decommit(void * addr, size_t size);

/// `MADV_FREE`. Returns true on error or if unsupported at run time.
/// jemalloc: pages_purge_lazy
bool purgeLazy(void * addr, size_t size);

/// Purge so that the pages read as zeros afterwards. Returns true on error.
/// jemalloc: pages_purge_forced
bool purgeForced(void * addr, size_t size);

/// `MADV_HUGEPAGE` on a hugepage-aligned range. Returns true on error.
/// jemalloc: pages_huge
bool huge(void * addr, size_t size);

/// `MADV_NOHUGEPAGE` on a hugepage-aligned range. Returns true on error.
/// jemalloc: pages_nohuge
bool nohuge(void * addr, size_t size);

/// `MADV_COLLAPSE` (not configured on any supported platform: always fails).
/// jemalloc: pages_collapse
bool collapse(void * addr, size_t size);

/// Exclude from core dumps. Returns true on error.
/// jemalloc: pages_dontdump
bool dontDump(void * addr, size_t size);

/// Include in core dumps again. Returns true on error.
/// jemalloc: pages_dodump
bool doDump(void * addr, size_t size);

/// Apply `opt.thp` to a new mapping (no-op with the default `thp` option).
/// jemalloc: pages_set_thp_state
void setThpState(void * ptr, size_t size);

/// Make the guard pages at `head` and/or `tail` (either may be null) inaccessible.
/// jemalloc: pages_mark_guards
void markGuards(void * head, void * tail);

/// jemalloc: pages_unmark_guards
void unmarkGuards(void * head, void * tail);

/// Boot-time probes: page size, `MADV_DONTNEED` zeroing, overcommit, THP mode, `MADV_FREE` support.
/// Returns true on error.
/// jemalloc: pages_boot
bool boot();

/// Introspection for tests.
bool osOvercommits();
int mmapFlags();
bool madviseDontNeedZerosIsFaulty();
bool canPurgeLazyRuntime();

}

/// jemalloc: extent_alloc_mmap
void * extentAllocMmap(void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit);

/// Returns true if the memory was not deallocated (with `opt_retain`).
/// jemalloc: extent_dalloc_mmap
bool extentDallocMmap(void * addr, size_t size);

}
