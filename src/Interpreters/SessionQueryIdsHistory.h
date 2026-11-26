#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <deque>
#include <mutex>
#include <vector>

namespace DB
{

/// Per-session history of the query ids of non-internal queries executed in the session.
/// Owned by the session Context and exposed through the `system.session_query_ids` table.
class SessionQueryIdsHistory
{
public:
    struct Entry
    {
        UInt64 sequence_number;
        String query_id;
    };

    using Entries = std::vector<Entry>;

    /// Records a query id, evicting the oldest entries when the history grows beyond max_size.
    void add(const String & query_id, UInt64 max_size);

    Entries getEntries() const;

    /// The sequence counter is not reset, so sequence numbers are never reused within a session.
    void clear();

private:
    mutable std::mutex mutex;
    std::deque<Entry> entries TSA_GUARDED_BY(mutex);
    UInt64 next_sequence_number TSA_GUARDED_BY(mutex) = 1;
};

}
