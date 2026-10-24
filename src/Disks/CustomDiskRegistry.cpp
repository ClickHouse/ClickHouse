#include <Disks/CustomDiskRegistry.h>

#include <Disks/IDisk.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DISK;
}

static void shutdownDisk(const String & name, const DiskPtr & disk, const LoggerPtr & log)
{
    try
    {
        disk->shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "while shutting down custom disk " + backQuote(name));
    }
}

/// The filesystem cache of a `cache` disk is registered under the name of the disk, and it would outlive the disk and keep
/// the name taken. Called when the disk has been shut down and destroyed, so the cache is not used by it anymore.
static void releaseFilesystemCacheName(const String & name, const LoggerPtr & log)
{
    try
    {
        FileCacheFactory::instance().removeNameOfReleasedDisk(name);
    }
    catch (...)
    {
        tryLogCurrentException(log, "while releasing the filesystem cache of custom disk " + backQuote(name));
    }
}

CustomDiskRegistry::CustomDiskRegistry()
    : log(getLogger("CustomDiskRegistry"))
{
}

CustomDiskRegistry::~CustomDiskRegistry()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

CustomDiskRegistrationPtr CustomDiskRegistry::getOrCreate(const String & name, CustomDiskRegistrations & nested)
{
    std::lock_guard lock(mutex);

    auto & weak_registration = registrations[name];
    auto registration = weak_registration.lock();
    if (!registration)
    {
        registration = std::make_shared<CustomDiskRegistration>(name, std::move(nested));
        weak_registration = registration;
    }
    return registration;
}

CustomDiskRegistrationPtr CustomDiskRegistry::tryGet(const String & name) const
{
    std::lock_guard lock(mutex);

    auto it = registrations.find(name);
    if (it == registrations.end())
        return nullptr;

    auto registration = it->second.lock();
    /// The last registration is being destroyed, and the disk is about to be unregistered. Taking no
    /// registration and using the disk anyway would leave the user of the disk without it.
    if (!registration)
        throw Exception(ErrorCodes::UNKNOWN_DISK,
            "Custom disk {} is being unregistered, because the last table or database using it is gone", backQuote(name));
    return registration;
}

bool CustomDiskRegistry::canRelease(const String & name) const
{
    std::lock_guard lock(mutex);

    auto it = registrations.find(name);
    return it != registrations.end() && it->second.expired();
}

void CustomDiskRegistry::release(const String & name)
{
    std::lock_guard lock(mutex);

    registrations.erase(name);
    names_pending_shutdown.insert(name);
}

void CustomDiskRegistry::rememberDefinitionLoadedFromExistingMetadata(UInt128 settings_hash)
{
    std::lock_guard lock(mutex);
    definitions_loaded_from_existing_metadata.insert(settings_hash);
}

bool CustomDiskRegistry::isDefinitionLoadedFromExistingMetadata(UInt128 settings_hash) const
{
    std::lock_guard lock(mutex);
    return definitions_loaded_from_existing_metadata.contains(settings_hash);
}

void CustomDiskRegistry::scheduleShutdown(const String & name, DiskPtr disk)
{
    DiskToShutdown disk_to_shutdown{name, std::move(disk)};

    if (disk_to_shutdown.disk)
    {
        std::lock_guard lock(mutex);
        if (!shutdown_called)
        {
            try
            {
                if (!thread)
                    thread = std::make_unique<ThreadFromGlobalPoolNoTracingContextPropagation>([this] { run(); });
                disks_to_shutdown.push_back(std::move(disk_to_shutdown));
            }
            catch (...)
            {
                /// Do not leave the name pending forever, `waitForShutdown` would never return.
                removePendingName(name);
                shutdown_finished_cv.notify_all();
                throw;
            }
            disks_to_shutdown_cv.notify_one();
            return;
        }
    }

    /// There is nothing to shut down, or the server is shutting down and there is nothing to wait for anymore.
    finishShutdown(std::move(disk_to_shutdown));
}

/// TSA_NO_THREAD_SAFETY_ANALYSIS because TSA does not support `std::unique_lock` used with `std::condition_variable`.
void CustomDiskRegistry::waitForShutdown(const String & name, UInt128 settings_hash) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    /// A disk is usually referenced only briefly after the last table using it is gone.
    static constexpr auto max_wait_for_referenced_disk = std::chrono::seconds(10);
    /// A disk with the same definition can be used again, but a new one starts with fresh metadata (e.g. after
    /// a failed attach that left the old one with stale metadata), so it is preferred if the wait is short.
    static constexpr auto max_wait_for_referenced_disk_with_same_definition = std::chrono::seconds(1);

    std::unique_lock lock(mutex);
    shutdown_finished_cv.wait(lock, [&]() TSA_NO_THREAD_SAFETY_ANALYSIS { return !names_pending_shutdown.contains(name); });

    auto it = lingering_disks.find(name);
    if (it == lingering_disks.end())
        return;

    const bool same_definition = it->second->getCustomDiskSettings() == settings_hash;
    const auto max_wait = same_definition ? max_wait_for_referenced_disk_with_same_definition : max_wait_for_referenced_disk;

    auto can_create = [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return !names_pending_shutdown.contains(name) && !lingering_disks.contains(name);
    };

    if (!shutdown_finished_cv.wait_for(lock, max_wait, can_create) && !same_definition && !names_pending_shutdown.contains(name))
        LOG_WARNING(log, "Custom disk {} released earlier, with another definition, is still referenced after {} seconds",
            backQuote(name), max_wait_for_referenced_disk.count());
}

std::optional<DiskPtr> CustomDiskRegistry::tryTakeReleasedDisk(const String & name)
{
    std::lock_guard lock(mutex);

    if (names_pending_shutdown.contains(name))
        return std::nullopt;

    auto it = lingering_disks.find(name);
    if (it == lingering_disks.end())
        return DiskPtr{};

    DiskPtr disk = std::move(it->second);
    lingering_disks.erase(it);
    LOG_DEBUG(log, "Custom disk {} is used again before it was shut down", backQuote(name));
    return disk;
}

void CustomDiskRegistry::shutdown()
{
    std::unique_ptr<ThreadFromGlobalPoolNoTracingContextPropagation> thread_to_join;
    {
        std::lock_guard lock(mutex);
        shutdown_called = true;
        thread_to_join = std::move(thread);
        disks_to_shutdown_cv.notify_one();
    }

    if (thread_to_join)
        thread_to_join->join();

    /// Like the disks of the disk selector, the disks still referenced by something are shut down when the server shuts down.
    std::map<String, DiskPtr> remaining_disks;
    {
        std::lock_guard lock(mutex);
        remaining_disks.swap(lingering_disks);
    }
    for (const auto & [name, disk] : remaining_disks)
        shutdownDisk(name, disk, log);
}

/// TSA_NO_THREAD_SAFETY_ANALYSIS because TSA does not support `std::unique_lock` used with `std::condition_variable`.
void CustomDiskRegistry::run() TSA_NO_THREAD_SAFETY_ANALYSIS
{
    DB::setThreadName(ThreadName::CUSTOM_DISK_SHUTDOWN);

    /// Nothing notifies about the last other reference to a lingering disk being dropped, so they are checked periodically.
    static constexpr auto lingering_disks_check_period = std::chrono::milliseconds(100);

    std::unique_lock lock(mutex);
    while (true)
    {
        auto has_work = [this]() TSA_NO_THREAD_SAFETY_ANALYSIS { return shutdown_called || !disks_to_shutdown.empty(); };
        if (lingering_disks.empty())
            disks_to_shutdown_cv.wait(lock, has_work);
        else
            disks_to_shutdown_cv.wait_for(lock, lingering_disks_check_period, has_work);

        /// The scheduled disks are shut down even when the server is shutting down: the disks
        /// released while the tables are being shut down are not in the disk selector anymore.
        if (!disks_to_shutdown.empty())
        {
            DiskToShutdown disk_to_shutdown = std::move(disks_to_shutdown.front());
            disks_to_shutdown.pop_front();

            lock.unlock();
            finishShutdown(std::move(disk_to_shutdown));
            lock.lock();
            continue;
        }

        shutdownUnreferencedLingeringDisks(lock);

        if (shutdown_called)
            return;
    }
}

void CustomDiskRegistry::finishShutdown(DiskToShutdown disk_to_shutdown)
{
    if (auto disk = std::move(disk_to_shutdown.disk))
    {
        bool referenced = false;
        {
            std::lock_guard lock(mutex);
            /// `shutdown` makes the disk reject further requests, so it may only be called when nothing can
            /// use the disk anymore. The disk is no longer reachable by name, so no new reference to it can
            /// appear, and holding the only one left means there is no user of it either. Otherwise the
            /// disk waits until the other references are gone. A new disk with the same definition is not
            /// created meanwhile: this one is used again instead, see `tryTakeReleasedDisk`.
            if (disk.use_count() != 1)
            {
                referenced = true;
                LOG_DEBUG(log, "Custom disk {} is still referenced after being unregistered, it will be shut down when the references are gone",
                    backQuote(disk_to_shutdown.name));
                chassert(!lingering_disks.contains(disk_to_shutdown.name));
                lingering_disks[disk_to_shutdown.name] = std::move(disk);
                disks_to_shutdown_cv.notify_one();
            }
        }

        if (!referenced)
        {
            shutdownDisk(disk_to_shutdown.name, disk, log);
            /// The reference is dropped before the next disk is taken: that may be the disk this one
            /// wraps, which is shut down only if nothing else references it.
            disk.reset();
            releaseFilesystemCacheName(disk_to_shutdown.name, log);
        }
    }

    std::lock_guard lock(mutex);
    removePendingName(disk_to_shutdown.name);
    shutdown_finished_cv.notify_all();
}

/// TSA_NO_THREAD_SAFETY_ANALYSIS because TSA does not support `std::unique_lock`.
void CustomDiskRegistry::shutdownUnreferencedLingeringDisks(std::unique_lock<std::mutex> & lock) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    std::vector<DiskToShutdown> unreferenced_disks;
    for (auto it = lingering_disks.begin(); it != lingering_disks.end();)
    {
        /// A disk is referenced by the disks wrapping it, so it is not taken together with them, but on a later check.
        if (it->second.use_count() == 1)
        {
            names_pending_shutdown.insert(it->first);
            unreferenced_disks.push_back({it->first, std::move(it->second)});
            it = lingering_disks.erase(it);
        }
        else
        {
            ++it;
        }
    }

    if (unreferenced_disks.empty())
        return;

    lock.unlock();
    for (auto & disk_to_shutdown : unreferenced_disks)
    {
        shutdownDisk(disk_to_shutdown.name, disk_to_shutdown.disk, log);
        disk_to_shutdown.disk.reset();
        releaseFilesystemCacheName(disk_to_shutdown.name, log);
    }
    lock.lock();

    for (const auto & disk_to_shutdown : unreferenced_disks)
        removePendingName(disk_to_shutdown.name);
    shutdown_finished_cv.notify_all();
}

void CustomDiskRegistry::removePendingName(const String & name)
{
    auto it = names_pending_shutdown.find(name);
    chassert(it != names_pending_shutdown.end());
    if (it != names_pending_shutdown.end())
        names_pending_shutdown.erase(it);
}

}
