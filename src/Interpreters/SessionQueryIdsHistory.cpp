#include <Interpreters/SessionQueryIdsHistory.h>

namespace DB
{

void SessionQueryIdsHistory::add(const String & query_id, UInt64 max_size)
{
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
    std::lock_guard lock(mutex);
    entries.clear();
}

}
