/// Darwin malloc zone (jemalloc: `src/zone.c`, compiled only on Darwin).
///
/// The allocator registers itself as a malloc zone named "jemalloc_zone" and promotes it to be the default zone, so
/// that `malloc`/`free` of the system libc (and everything else on Darwin) end up here. ClickHouse
/// (`src/Common/malloc.cpp`, `initializeJemallocZoneMemoryTracking`) finds the zone by name and patches its callbacks
/// in place, so `jemalloc_zone` must stay a writable static. `zone_register` is exported unprefixed with C linkage
/// (ClickHouse calls it explicitly, `src/Common/AllocationInterceptors.cpp`) and also runs as a constructor.

#if !defined(__APPLE__)
#    error "This source file is for zones on Darwin (OS X)."
#endif

#include <allocator/Common.h>
#include <allocator/Config.h>
#include <allocator/Frontend.h>
#include <allocator/Init.h>
#include <allocator/Mutex.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include <mach/mach.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

extern "C"
{
#include <jemalloc/jemalloc_defs.h>
#include <jemalloc/jemalloc_macros.h>
#include <jemalloc/jemalloc_protos.h>
}

static_assert(jemalloc::config::zone);

namespace jemalloc
{

/// Definitions of the following structs in malloc/malloc.h might be too old for the built binary to run on newer
/// versions of OSX. So use the newest possible version of those structs. (They live in this namespace so that they do
/// not clash with the definitions of <malloc/malloc.h> that ClickHouse includes elsewhere; only the layout matters.)
struct MallocIntrospection;

/// jemalloc: malloc_zone_t (`struct _malloc_zone_t`)
struct MallocZone
{
    void * reserved1;
    void * reserved2;
    size_t (*size)(MallocZone *, const void *);
    void * (*malloc)(MallocZone *, size_t);
    void * (*calloc)(MallocZone *, size_t, size_t);
    void * (*valloc)(MallocZone *, size_t);
    void (*free)(MallocZone *, void *);
    void * (*realloc)(MallocZone *, void *, size_t);
    void (*destroy)(MallocZone *);
    const char * zone_name;
    unsigned (*batch_malloc)(MallocZone *, size_t, void **, unsigned);
    void (*batch_free)(MallocZone *, void **, unsigned);
    MallocIntrospection * introspect;
    unsigned version;
    void * (*memalign)(MallocZone *, size_t, size_t);
    void (*free_definite_size)(MallocZone *, void *, size_t);
    size_t (*pressure_relief)(MallocZone *, size_t);
};

/// jemalloc: vm_range_t
struct VmRange
{
    vm_address_t address;
    vm_size_t size;
};

/// jemalloc: malloc_statistics_t
struct MallocStatistics
{
    unsigned blocks_in_use;
    size_t size_in_use;
    size_t max_size_in_use;
    size_t size_allocated;
};

/// jemalloc: memory_reader_t
using MemoryReader = kern_return_t(task_t, vm_address_t, vm_size_t, void **);

/// jemalloc: vm_range_recorder_t
using VmRangeRecorder = void(task_t, void *, unsigned type, VmRange *, unsigned);

/// jemalloc: malloc_introspection_t
struct MallocIntrospection
{
    kern_return_t (*enumerator)(task_t, void *, unsigned, vm_address_t, MemoryReader, VmRangeRecorder);
    size_t (*good_size)(MallocZone *, size_t);
    boolean_t (*check)(MallocZone *);
    void (*print)(MallocZone *, boolean_t);
    void (*log)(MallocZone *, void *);
    void (*force_lock)(MallocZone *);
    void (*force_unlock)(MallocZone *);
    void (*statistics)(MallocZone *, MallocStatistics *);
    boolean_t (*zone_locked)(MallocZone *);
    boolean_t (*enable_discharge_checking)(MallocZone *);
    boolean_t (*disable_discharge_checking)(MallocZone *);
    void (*discharge)(MallocZone *, void *);
#if defined(__BLOCKS__)
    void (*enumerate_discharged_pointers)(MallocZone *, void (^)(void *, void *));
#else
    void * enumerate_unavailable_without_blocks;
#endif
    void (*reinit_lock)(MallocZone *);
};

}

extern "C"
{
kern_return_t malloc_get_all_zones(task_t, jemalloc::MemoryReader, vm_address_t **, unsigned *);
jemalloc::MallocZone * malloc_default_zone();
void malloc_zone_register(jemalloc::MallocZone * zone);
void malloc_zone_unregister(jemalloc::MallocZone * zone);
/// The malloc_default_purgeable_zone() function is only available on >= 10.6. We need to check whether it is present
/// at runtime, thus the weak_import.
jemalloc::MallocZone * malloc_default_purgeable_zone() __attribute__((weak_import));
}

namespace jemalloc
{

namespace
{

/// --- Data -----------------------------------------------------------------------------------------------------------

constinit MallocZone * default_zone = nullptr;
constinit MallocZone * purgeable_zone = nullptr;
/// Writable: ClickHouse patches the callbacks in place.
constinit MallocZone jemalloc_zone{};
constinit MallocIntrospection jemalloc_zone_introspect{};
constinit pid_t zone_force_lock_pid = -1;

/// --- Functions ------------------------------------------------------------------------------------------------------

/// There appear to be places within Darwin (such as setenv(3)) that cause calls to this function with pointers that
/// *no* zone owns. If we knew that all pointers were owned by *some* zone, we could split our zone into two parts,
/// and use one as the default allocator and the other as the default deallocator/reallocator. Since that will not
/// work in practice, we must check all pointers to assure that they reside within a mapped extent before determining
/// size.
/// jemalloc: zone_size
size_t zoneSize(MallocZone * /*zone*/, const void * ptr)
{
    return ivsalloc(ThreadState::tsdnFetch(), ptr);
}

/// jemalloc: zone_malloc
void * zoneMalloc(MallocZone * /*zone*/, size_t size)
{
    return je_malloc(size);
}

/// jemalloc: zone_calloc
void * zoneCalloc(MallocZone * /*zone*/, size_t num, size_t size)
{
    return je_calloc(num, size);
}

/// jemalloc: zone_valloc
void * zoneValloc(MallocZone * /*zone*/, size_t size)
{
    void * ret = nullptr; /// Assignment avoids useless compiler warning.
    je_posix_memalign(&ret, PAGE, size);
    return ret;
}

/// jemalloc: zone_free
void zoneFree(MallocZone * /*zone*/, void * ptr)
{
    if (ivsalloc(ThreadState::tsdnFetch(), ptr) != 0)
    {
        je_free(ptr);
        return;
    }

    ::free(ptr);
}

/// jemalloc: zone_realloc
void * zoneRealloc(MallocZone * /*zone*/, void * ptr, size_t size)
{
    if (ivsalloc(ThreadState::tsdnFetch(), ptr) != 0)
        return je_realloc(ptr, size);

    return ::realloc(ptr, size);
}

/// jemalloc: zone_memalign
void * zoneMemalign(MallocZone * /*zone*/, size_t alignment, size_t size)
{
    void * ret = nullptr; /// Assignment avoids useless compiler warning.
    je_posix_memalign(&ret, alignment, size);
    return ret;
}

/// jemalloc: zone_free_definite_size
void zoneFreeDefiniteSize(MallocZone * /*zone*/, void * ptr, [[maybe_unused]] size_t size)
{
    size_t alloc_size = ivsalloc(ThreadState::tsdnFetch(), ptr);
    if (alloc_size != 0)
    {
        JE_ASSERT(alloc_size == size);
        je_free(ptr);
        return;
    }

    ::free(ptr);
}

/// This function should never be called.
/// jemalloc: zone_destroy
void zoneDestroy(MallocZone * /*zone*/)
{
    JE_NOT_REACHED();
}

/// jemalloc: zone_batch_malloc
unsigned zoneBatchMalloc(MallocZone * /*zone*/, size_t size, void ** results, unsigned num_requested)
{
    unsigned i;
    for (i = 0; i < num_requested; ++i)
    {
        results[i] = je_malloc(size);
        if (!results[i])
            break;
    }
    return i;
}

/// jemalloc: zone_batch_free
void zoneBatchFree(MallocZone * zone, void ** to_be_freed, unsigned num_to_be_freed)
{
    for (unsigned i = 0; i < num_to_be_freed; ++i)
    {
        zoneFree(zone, to_be_freed[i]);
        to_be_freed[i] = nullptr;
    }
}

/// jemalloc: zone_pressure_relief
size_t zonePressureRelief(MallocZone * /*zone*/, size_t /*goal*/)
{
    return 0;
}

/// jemalloc: zone_good_size
size_t zoneGoodSize(MallocZone * /*zone*/, size_t size)
{
    if (size == 0)
        size = 1;
    return sz::s2u(size);
}

/// jemalloc: zone_enumerator
kern_return_t zoneEnumerator(
    task_t /*task*/, void * /*data*/, unsigned /*type_mask*/, vm_address_t /*zone_address*/, MemoryReader /*reader*/,
    VmRangeRecorder /*recorder*/)
{
    return KERN_SUCCESS;
}

/// jemalloc: zone_check
boolean_t zoneCheck(MallocZone * /*zone*/)
{
    return true;
}

/// jemalloc: zone_print
void zonePrint(MallocZone * /*zone*/, boolean_t /*verbose*/)
{
}

/// jemalloc: zone_log
void zoneLog(MallocZone * /*zone*/, void * /*address*/)
{
}

/// jemalloc: zone_force_lock
void zoneForceLock(MallocZone * /*zone*/)
{
    if (isThreaded())
    {
        /// See the note in `zoneForceUnlock`, below, to see why we need this.
        JE_ASSERT(zone_force_lock_pid == -1);
        zone_force_lock_pid = getpid();
        jemallocPrefork();
    }
}

/// `zone_force_lock` and `zone_force_unlock` are the entry points to the forking machinery on OS X. The tricky thing
/// is, the child is not allowed to unlock mutexes locked in the parent, even if owned by the forking thread (and the
/// mutex type we use in OS X will fail an assert if we try). In the child, we can get away with reinitializing all
/// the mutexes, which has the effect of unlocking them. In the parent, doing this would mean we wouldn't wake any
/// waiters blocked on the mutexes we unlock. So, we record the pid of the current thread in `zone_force_lock`, and
/// use that to detect if we're in the parent or child here, to decide which unlock logic we need.
/// jemalloc: zone_force_unlock
void zoneForceUnlock(MallocZone * /*zone*/)
{
    if (isThreaded())
    {
        JE_ASSERT(zone_force_lock_pid != -1);
        if (getpid() == zone_force_lock_pid)
            jemallocPostforkParent();
        else
            jemallocPostforkChild();
        zone_force_lock_pid = -1;
    }
}

/// We make no effort to actually fill the values.
/// jemalloc: zone_statistics
void zoneStatistics(MallocZone * /*zone*/, MallocStatistics * stats)
{
    stats->blocks_in_use = 0;
    stats->size_in_use = 0;
    stats->max_size_in_use = 0;
    stats->size_allocated = 0;
}

/// Pretend no lock is being held.
/// jemalloc: zone_locked
boolean_t zoneLocked(MallocZone * /*zone*/)
{
    return false;
}

/// As of OSX 10.12, this function is only used when force_unlock would be used if the zone version were < 9. So just
/// use force_unlock.
/// jemalloc: zone_reinit_lock
void zoneReinitLock(MallocZone * zone)
{
    zoneForceUnlock(zone);
}

/// jemalloc: zone_init
void zoneInit()
{
    jemalloc_zone.size = zoneSize;
    jemalloc_zone.malloc = zoneMalloc;
    jemalloc_zone.calloc = zoneCalloc;
    jemalloc_zone.valloc = zoneValloc;
    jemalloc_zone.free = zoneFree;
    jemalloc_zone.realloc = zoneRealloc;
    jemalloc_zone.destroy = zoneDestroy;
    jemalloc_zone.zone_name = "jemalloc_zone";
    jemalloc_zone.batch_malloc = zoneBatchMalloc;
    jemalloc_zone.batch_free = zoneBatchFree;
    jemalloc_zone.introspect = &jemalloc_zone_introspect;
    jemalloc_zone.version = 9;
    jemalloc_zone.memalign = zoneMemalign;
    jemalloc_zone.free_definite_size = zoneFreeDefiniteSize;
    jemalloc_zone.pressure_relief = zonePressureRelief;

    jemalloc_zone_introspect.enumerator = zoneEnumerator;
    jemalloc_zone_introspect.good_size = zoneGoodSize;
    jemalloc_zone_introspect.check = zoneCheck;
    jemalloc_zone_introspect.print = zonePrint;
    jemalloc_zone_introspect.log = zoneLog;
    jemalloc_zone_introspect.force_lock = zoneForceLock;
    jemalloc_zone_introspect.force_unlock = zoneForceUnlock;
    jemalloc_zone_introspect.statistics = zoneStatistics;
    jemalloc_zone_introspect.zone_locked = zoneLocked;
    jemalloc_zone_introspect.enable_discharge_checking = nullptr;
    jemalloc_zone_introspect.disable_discharge_checking = nullptr;
    jemalloc_zone_introspect.discharge = nullptr;
#if defined(__BLOCKS__)
    jemalloc_zone_introspect.enumerate_discharged_pointers = nullptr;
#else
    jemalloc_zone_introspect.enumerate_unavailable_without_blocks = nullptr;
#endif
    jemalloc_zone_introspect.reinit_lock = zoneReinitLock;
}

/// On OSX 10.12, `malloc_default_zone` returns a special zone that is not present in the list of registered zones.
/// That zone uses a "lite zone" if one is present (apparently enabled when malloc stack logging is enabled), or the
/// first registered zone otherwise. In practice this means unless malloc stack logging is enabled, the first
/// registered zone is the default. So get the list of zones to get the first one, instead of relying on
/// `malloc_default_zone`.
/// jemalloc: zone_default_get
MallocZone * zoneDefaultGet()
{
    MallocZone ** zones = nullptr;
    unsigned int num_zones = 0;

    if (KERN_SUCCESS != malloc_get_all_zones(0, nullptr, reinterpret_cast<vm_address_t **>(&zones), &num_zones))
    {
        /// Reset the value in case the failure happened after it was set.
        num_zones = 0;
    }

    if (num_zones)
        return zones[0];

    return malloc_default_zone();
}

/// As written, this function can only promote `jemalloc_zone`.
/// jemalloc: zone_promote
void zonePromote()
{
    MallocZone * zone;

    do
    {
        /// Unregister and reregister the default zone. On OSX >= 10.6, unregistering takes the last registered zone
        /// and places it at the location of the specified zone. Unregistering the default zone thus makes the last
        /// registered one the default. On OSX < 10.6, unregistering shifts all registered zones. The first
        /// registered zone then becomes the default.
        malloc_zone_unregister(default_zone);
        malloc_zone_register(default_zone);

        /// On OSX 10.6, having the default purgeable zone appear before the default zone makes some things crash
        /// because it thinks it owns the default zone allocated pointers. We thus unregister/re-register it in order
        /// to ensure it's always after the default zone. On OSX < 10.6, there is no purgeable zone, so this does
        /// nothing. On OSX >= 10.6, unregistering replaces the purgeable zone with the last registered zone above,
        /// i.e. the default zone. Registering it again then puts it at the end, obviously after the default zone.
        if (purgeable_zone != nullptr)
        {
            malloc_zone_unregister(purgeable_zone);
            malloc_zone_register(purgeable_zone);
        }

        zone = zoneDefaultGet();
    } while (zone != &jemalloc_zone);
}

}

}

/// Exported unprefixed: ClickHouse calls it explicitly (`src/Common/AllocationInterceptors.cpp`).
extern "C" __attribute__((visibility("default"))) void zone_register();

/// jemalloc: zone_register
extern "C" __attribute__((constructor, visibility("default"))) void zone_register()
{
    using namespace jemalloc;

    /// If something else replaced the system default zone allocator, don't register jemalloc's.
    default_zone = zoneDefaultGet();
    if (!default_zone->zone_name || strcmp(default_zone->zone_name, "DefaultMallocZone") != 0)
        return;

    /// The default purgeable zone is created lazily by OSX's libc. It uses the default zone when it is created for
    /// "small" allocations (< 15 KiB), but assumes the default zone is a scalable_zone. This obviously fails when the
    /// default zone is the jemalloc zone, so `malloc_default_purgeable_zone` is called beforehand so that the default
    /// purgeable zone is created when the default zone is still a scalable_zone. As purgeable zones only exist on
    /// >= 10.6, we need to check for the existence of `malloc_default_purgeable_zone` at run time.
    purgeable_zone = (malloc_default_purgeable_zone == nullptr) ? nullptr : malloc_default_purgeable_zone();

    /// Register the custom zone. At this point it won't be the default.
    zoneInit();
    malloc_zone_register(&jemalloc_zone);

    /// Promote the custom zone to be default.
    zonePromote();
}
