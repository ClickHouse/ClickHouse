#include <Interpreters/SessionQueryIdsHistory.h>
#include <Common/MemoryTrackerSwitcher.h>

namespace DB
{

void SessionQueryIdsHistory::add(const String & query_id, UInt64 max_size)
{
    /// Entries and deque storage belong to the session, including eviction of older entries.
    MemoryTrackerSwitcher history_memory_scope(&total_memory_tracker);
    std::lock_guard lock(mutex);
    entries.emplace_back(Entry{next_sequence_number++, query_id});
    while (entries.size() > max_size)
        entries.pop_front();
}

SessionQueryIdsHistory::Entries SessionQueryIdsHistory::getEntries() const
{
    std::lock_guard lock(mutex);
    return {entries.begin(), entries.end()};
}

void SessionQueryIdsHistory::clear()
{
    MemoryTrackerSwitcher history_memory_scope(&total_memory_tracker);
    std::lock_guard lock(mutex);
    entries.clear();
}

}
