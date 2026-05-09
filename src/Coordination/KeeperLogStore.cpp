#include <Coordination/KeeperLogStore.h>
#include <Common/ProfiledLocks.h>
#include <IO/CompressionMethod.h>
#include <Disks/DiskLocal.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event KeeperChangelogLockWaitMicroseconds;
}

namespace DB
{

KeeperLogStore::KeeperLogStore(LogFileSettings log_file_settings, FlushSettings flush_settings, KeeperContextPtr keeper_context)
    : log(getLogger("KeeperLogStore")), changelog(log, log_file_settings, flush_settings, keeper_context)
{
    if (log_file_settings.force_sync)
        LOG_INFO(log, "force_sync enabled");
    else
        LOG_INFO(log, "force_sync disabled");
}

uint64_t KeeperLogStore::start_index() const
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.getStartIndex();
}

void KeeperLogStore::init(uint64_t last_commited_log_index, uint64_t logs_to_keep)
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    changelog.readChangelogAndInitWriter(last_commited_log_index, logs_to_keep);
}

uint64_t KeeperLogStore::next_slot() const
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.getNextEntryIndex();
}

nuraft::ptr<nuraft::log_entry> KeeperLogStore::last_entry() const
{
    /// Exclusive: getLastEntry -> getEntry may mutate LogEntryStorage::first_log_entry.
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.getLastEntry();
}

uint64_t KeeperLogStore::append(nuraft::ptr<nuraft::log_entry> & entry)
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    uint64_t idx = changelog.getNextEntryIndex();
    changelog.appendEntry(idx, entry);
    return idx;
}


void KeeperLogStore::write_at(uint64_t index, nuraft::ptr<nuraft::log_entry> & entry)
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    changelog.writeAt(index, entry);
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> KeeperLogStore::log_entries(uint64_t start, uint64_t end)
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.getLogEntriesBetween(start, end);
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
KeeperLogStore::log_entries_ext(uint64_t start, uint64_t end, int64_t batch_size_hint_in_bytes)
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.getLogEntriesBetween(start, end, batch_size_hint_in_bytes);
}

nuraft::ptr<nuraft::log_entry> KeeperLogStore::entry_at(uint64_t index)
{
    /// Exclusive: entryAt -> getEntry may mutate LogEntryStorage::first_log_entry.
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.entryAt(index);
}

bool KeeperLogStore::is_conf(uint64_t index)
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.isConfigLog(index);
}

uint64_t KeeperLogStore::term_at(uint64_t index)
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.termAt(index);
}

nuraft::ptr<nuraft::buffer> KeeperLogStore::pack(uint64_t index, int32_t cnt)
{
    /// Exclusive: serializeEntriesToBuffer -> getEntry may mutate LogEntryStorage::first_log_entry.
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.serializeEntriesToBuffer(index, cnt);
}

bool KeeperLogStore::compact(uint64_t last_log_index)
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    changelog.compact(last_log_index);
    return true;
}

bool KeeperLogStore::flush()
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.flush();
}

void KeeperLogStore::apply_pack(uint64_t index, nuraft::buffer & pack)
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    changelog.applyEntriesFromBuffer(index, pack);
}

uint64_t KeeperLogStore::size() const
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.size();
}

void KeeperLogStore::end_of_append_batch(uint64_t /*start_index*/, uint64_t /*count*/)
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    changelog.flushAsync();
}

nuraft::ptr<nuraft::log_entry> KeeperLogStore::getLatestConfigChange() const
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.getLatestConfigChange();
}

void KeeperLogStore::shutdownChangelog()
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    changelog.shutdown();
}

bool KeeperLogStore::flushChangelogAndShutdown()
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    if (changelog.isInitialized())
        changelog.flush();
    changelog.shutdown();
    return true;
}

uint64_t KeeperLogStore::last_durable_index()
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    return changelog.lastDurableIndex();
}

void KeeperLogStore::setRaftServer(const nuraft::ptr<nuraft::raft_server> & raft_server)
{
    ProfiledExclusiveLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    changelog.setRaftServer(raft_server);
}

void KeeperLogStore::getKeeperLogInfo(KeeperLogInfo & log_info) const
{
    ProfiledSharedLock lock(changelog_lock, ProfileEvents::KeeperChangelogLockWaitMicroseconds);
    changelog.getKeeperLogInfo(log_info);
}

}
