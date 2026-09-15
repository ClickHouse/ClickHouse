#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>
#include <Core/UUID.h>

namespace DB
{

/** PartUUIDs is a uuid set to control query deduplication.
 * The object is used in query context in both direction:
 *  Server->Client to send all parts' UUIDs that have been read during the query
 *  Client->Server to ignored specified parts from being processed.
 *
 *  Current implementation assumes a user setting allow_experimental_query_deduplication=1 is set.
 */
struct PartUUIDs
{
public:
    /// Add new UUIDs if not duplicates found otherwise return duplicated UUIDs
    std::vector<UUID> add(const std::vector<UUID> & uuids);
    /// Get accumulated UUIDs
    std::vector<UUID> get() const;
    bool has(const UUID & uuid) const;

private:
    mutable std::mutex mutex;
    std::unordered_set<UUID> uuids;
};

using PartUUIDsPtr = std::shared_ptr<PartUUIDs>;

}
