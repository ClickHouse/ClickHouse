#include <Coordination/KeeperLogStore.h>

namespace DB
{

KeeperLogStore::KeeperLogStore(const std::string & changelogs_path, size_t rotate_interval_, bool force_sync_)
    : log(&Poco::Logger::get("KeeperLogStore"))
    , changelog(changelogs_path, rotate_interval_, log)
    , force_sync(force_sync_)
{
}

size_t KeeperLogStore::start_index() const
{
    std::lock_guard lock(changelog_lock);
    return changelog.getStartIndex();
}

void KeeperLogStore::init(size_t last_commited_log_index, size_t logs_to_keep)
{
    std::lock_guard lock(changelog_lock);
    changelog.readChangelogAndInitWriter(last_commited_log_index, logs_to_keep);
}

size_t KeeperLogStore::next_slot() const
{
    std::lock_guard lock(changelog_lock);
    return changelog.getNextEntryIndex();
}

nuraft::ptr<nuraft::log_entry> KeeperLogStore::last_entry() const
{
    std::lock_guard lock(changelog_lock);
    return changelog.getLastEntry();
}

size_t KeeperLogStore::append(nuraft::ptr<nuraft::log_entry> & entry)
{
    std::lock_guard lock(changelog_lock);
    size_t idx = changelog.getNextEntryIndex();
    changelog.appendEntry(idx, entry, force_sync);
    return idx;
}


void KeeperLogStore::write_at(size_t index, nuraft::ptr<nuraft::log_entry> & entry)
{
    std::lock_guard lock(changelog_lock);
    changelog.writeAt(index, entry, force_sync);
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> KeeperLogStore::log_entries(size_t start, size_t end)
{
    std::lock_guard lock(changelog_lock);
    return changelog.getLogEntriesBetween(start, end);
}

nuraft::ptr<nuraft::log_entry> KeeperLogStore::entry_at(size_t index)
{
    std::lock_guard lock(changelog_lock);
    return changelog.entryAt(index);
}

size_t KeeperLogStore::term_at(size_t index)
{
    std::lock_guard lock(changelog_lock);
    auto entry = changelog.entryAt(index);
    if (entry)
        return entry->get_term();
    return 0;
}

nuraft::ptr<nuraft::buffer> KeeperLogStore::pack(size_t index, int32_t cnt)
{
    std::lock_guard lock(changelog_lock);
    return changelog.serializeEntriesToBuffer(index, cnt);
}

bool KeeperLogStore::compact(size_t last_log_index)
{
    std::lock_guard lock(changelog_lock);
    changelog.compact(last_log_index);
    return true;
}

bool KeeperLogStore::flush()
{
    std::lock_guard lock(changelog_lock);
    changelog.flush();
    return true;
}

void KeeperLogStore::apply_pack(size_t index, nuraft::buffer & pack)
{
    std::lock_guard lock(changelog_lock);
    changelog.applyEntriesFromBuffer(index, pack, force_sync);
}

size_t KeeperLogStore::size() const
{
    std::lock_guard lock(changelog_lock);
    return changelog.size();
}

}
