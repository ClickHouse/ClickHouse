#pragma once

#include <Disks/CustomDiskRegistration.h>
#include <Common/Logger.h>
#include <Common/ThreadPool_fwd.h>
#include <base/defines.h>
#include <base/extended_types.h>

#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <set>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// The registrations of the custom disks of the server, and the background thread that shuts down
/// the disks that are no longer used. `shutdown` of a disk may take a while (it waits for the
/// background operations of the disk), and the last registration may be destroyed by any thread
/// (a query dropping a table, a thread finishing a query that held the last reference to a dropped
/// table, ...), so it is not done in place.
class CustomDiskRegistry
{
public:
    CustomDiskRegistry();
    ~CustomDiskRegistry();

    /// The registration of the disk `name`. If there is none yet, creates it, and the new
    /// registration takes over `nested`. Otherwise `nested` is left to the caller: destroying a
    /// registration may release a disk, which locks the mutexes the caller may be holding.
    CustomDiskRegistrationPtr getOrCreate(const String & name, CustomDiskRegistrations & nested);
    /// The registration of the disk `name`, or nullptr if there is none. Throws if the last registration
    /// of the disk is being destroyed: the disk is about to be unregistered, so it must not get a new user.
    CustomDiskRegistrationPtr tryGet(const String & name) const;
    /// Whether the last registration of the disk `name` is being destroyed. It is not if the disk
    /// has been registered again in the meantime, and then the disk has to stay.
    bool canRelease(const String & name) const;
    /// Removes the registration of the disk `name`, after `canRelease`. Until `scheduleShutdown` is
    /// done with the disk, `waitForShutdown` waits for it.
    void release(const String & name);

    /// Shuts down a released disk in the background, after the disks scheduled before it, once nothing
    /// else uses it. A disk wraps the disks nested in its definition, which are released after it, so
    /// they are scheduled after it as well. `disk` is nullptr if there was no disk to release.
    void scheduleShutdown(const String & name, DiskPtr disk);
    /// Waits until the disk `name` released earlier is no longer being shut down. A new disk created
    /// with the same name uses the same metadata and data, so it must not overlap with the old one.
    /// A released disk that is not shut down yet because something still references it is waited for
    /// to be shut down, but only for a while, and then `tryTakeReleasedDisk` returns it anyway. The wait
    /// is short if it has the same definition (`settings_hash`), because then it can be used again.
    void waitForShutdown(const String & name, UInt128 settings_hash);
    /// The released disk `name` that is not shut down yet because something still references it, to be
    /// used again instead of creating a new disk with the same name. nullptr if there is none.
    /// std::nullopt if the disk `name` is being shut down, and `waitForShutdown` has to be called first.
    std::optional<DiskPtr> tryTakeReleasedDisk(const String & name);

    /// A disk definition (`settings_hash`, see `IDisk::markDiskAsCustom`) loaded from existing metadata,
    /// e.g. at server start, is exempt from the checks of a new definition. It stays exempt after its disk
    /// is released, as if the disk was still registered, so that `UNDROP TABLE` or `DETACH` and `ATTACH`
    /// of a table or database using it keep working.
    void rememberDefinitionLoadedFromExistingMetadata(UInt128 settings_hash);
    bool isDefinitionLoadedFromExistingMetadata(UInt128 settings_hash) const;

    /// Waits for the scheduled shutdowns and shuts down the disks still waiting for their references to
    /// go away. The disks released after that are shut down in place.
    void shutdown();

private:
    struct DiskToShutdown
    {
        String name;
        DiskPtr disk;
    };

    void run();
    void finishShutdown(DiskToShutdown disk_to_shutdown);
    /// Shuts down the lingering disks that are not referenced by anything else anymore.
    void shutdownUnreferencedLingeringDisks(std::unique_lock<std::mutex> & lock);
    void removePendingName(const String & name) TSA_REQUIRES(mutex);

    LoggerPtr log;

    mutable std::mutex mutex;
    std::map<String, std::weak_ptr<CustomDiskRegistration>> registrations TSA_GUARDED_BY(mutex);
    std::deque<DiskToShutdown> disks_to_shutdown TSA_GUARDED_BY(mutex);
    std::condition_variable disks_to_shutdown_cv;
    /// The names of the released disks that are scheduled for shutdown or being shut down.
    std::multiset<String> names_pending_shutdown TSA_GUARDED_BY(mutex);
    std::condition_variable shutdown_finished_cv;
    /// The released disks that were still referenced by something else when their turn to be shut down
    /// came. They are shut down when the reference goes away, or used again if a table or database with
    /// the same definition appears before that.
    std::map<String, DiskPtr> lingering_disks TSA_GUARDED_BY(mutex);
    std::set<UInt128> definitions_loaded_from_existing_metadata TSA_GUARDED_BY(mutex);
    bool shutdown_called TSA_GUARDED_BY(mutex) = false;
    /// Started on the first scheduled shutdown.
    std::unique_ptr<ThreadFromGlobalPoolNoTracingContextPropagation> thread TSA_GUARDED_BY(mutex);
};

}
