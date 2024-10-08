#include <Coordination/KeeperLogStore.h>
#include <IO/CompressionMethod.h>
#include <Disks/DiskLocal.h>
#include <Common/logger_useful.h>

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
    std::lock_guard lock(changelog_lock);
    return changelog.getStartIndex();
}

void KeeperLogStore::init(uint64_t last_commited_log_index, uint64_t logs_to_keep)
{
    std::lock_guard lock(changelog_lock);
    changelog.readChangelogAndInitWriter(last_commited_log_index, logs_to_keep);
}

uint64_t KeeperLogStore::next_slot() const
{
    std::lock_guard lock(changelog_lock);
    return changelog.getNextEntryIndex();
}

nuraft::ptr<nuraft::log_entry> KeeperLogStore::last_entry() const
{
    std::lock_guard lock(changelog_lock);
    return changelog.getLastEntry();
}

uint64_t KeeperLogStore::append(nuraft::ptr<nuraft::log_entry> & entry)
{
    std::lock_guard lock(changelog_lock);
    uint64_t idx = changelog.getNextEntryIndex();
    changelog.appendEntry(idx, entry);
    return idx;
}


void KeeperLogStore::write_at(uint64_t index, nuraft::ptr<nuraft::log_entry> & entry)
{
    std::lock_guard lock(changelog_lock);
    changelog.writeAt(index, entry);
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> KeeperLogStore::log_entries(uint64_t start, uint64_t end)
{
    std::lock_guard lock(changelog_lock);
    return changelog.getLogEntriesBetween(start, end);
}

nuraft::ptr<nuraft::log_entry> KeeperLogStore::entry_at(uint64_t index)
{
    std::lock_guard lock(changelog_lock);
    return changelog.entryAt(index);
}

bool KeeperLogStore::is_conf(uint64_t index)
{
    std::lock_guard lock(changelog_lock);
    return changelog.isConfigLog(index);
}

uint64_t KeeperLogStore::term_at(uint64_t index)
{
    std::lock_guard lock(changelog_lock);
    return changelog.termAt(index);
}

nuraft::ptr<nuraft::buffer> KeeperLogStore::pack(uint64_t index, int32_t cnt)
{
    std::lock_guard lock(changelog_lock);
    return changelog.serializeEntriesToBuffer(index, cnt);
}

bool KeeperLogStore::compact(uint64_t last_log_index)
{
    std::lock_guard lock(changelog_lock);
    changelog.compact(last_log_index);
    return true;
}

bool KeeperLogStore::flush()
{
    std::lock_guard lock(changelog_lock);
    return changelog.flush();
}

void KeeperLogStore::apply_pack(uint64_t index, nuraft::buffer & pack)
{
    std::lock_guard lock(changelog_lock);
    changelog.applyEntriesFromBuffer(index, pack);
}

uint64_t KeeperLogStore::size() const
{
    std::lock_guard lock(changelog_lock);
    return changelog.size();
}

void KeeperLogStore::end_of_append_batch(uint64_t /*start_index*/, uint64_t /*count*/)
{
    std::lock_guard lock(changelog_lock);
    changelog.flushAsync();
}

nuraft::ptr<nuraft::log_entry> KeeperLogStore::getLatestConfigChange() const
{
    std::lock_guard lock(changelog_lock);
    return changelog.getLatestConfigChange();
}

void KeeperLogStore::shutdownChangelog()
{
    std::lock_guard lock(changelog_lock);
    changelog.shutdown();
}

bool KeeperLogStore::flushChangelogAndShutdown()
{
    std::lock_guard lock(changelog_lock);
    if (changelog.isInitialized())
        changelog.flush();
    changelog.shutdown();
    return true;
}

uint64_t KeeperLogStore::last_durable_index()
{
    std::lock_guard lock(changelog_lock);
    return changelog.lastDurableIndex();
}

void KeeperLogStore::setRaftServer(const nuraft::ptr<nuraft::raft_server> & raft_server)
{
    std::lock_guard lock(changelog_lock);
    changelog.setRaftServer(raft_server);
}

void KeeperLogStore::getKeeperLogInfo(KeeperLogInfo & log_info) const
{
    std::lock_guard lock(changelog_lock);
    changelog.getKeeperLogInfo(log_info);
}

}
