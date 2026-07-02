#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>


namespace DB
{

/// Records the exact set of object-storage objects that a query actually consumed while reading, keyed
/// by table UUID.
///
/// `StorageObjectStorage::getModificationHash` lists the objects behind a table independently of the
/// read, which opens a listing `A -> B -> A` race for `query_cache_use_only_when_data_was_not_changed`:
/// the pre-read check lists `{a}`, a matching object `b` appears and the query reads `{a, b}`, then `b`
/// is deleted before the finalization check lists `{a}` again. The pre/post hashes match even though the
/// cached result was produced from a different object set. To close it, the read captures the set it
/// actually consumed here, and `getModificationHash` hashes that captured set at finalization time
/// instead of re-listing - so a cached result is only kept when it matches the object set folded into
/// its cache key.
struct QueryConsumedObjectSets
{
    struct Object
    {
        String path;
        String etag;
        UInt64 size = 0;
        Int64 last_modified = 0;
        /// False when the read could not attach object metadata (e.g. no ETag). `getModificationHash`
        /// then fails closed for the table rather than risk an unsound comparison.
        bool has_metadata = false;
    };

    /// Called (possibly concurrently from several read streams) for every object the read consumes.
    void add(const UUID & table_uuid, Object object);

    /// The objects consumed for `table_uuid`, or nullopt if the read captured nothing for it (e.g. the
    /// table was not read, or this is the pre-read check that runs before any object was consumed).
    std::optional<std::vector<Object>> get(const UUID & table_uuid) const;

private:
    mutable std::mutex mutex;
    std::map<UUID, std::vector<Object>> objects_by_table;
};

using QueryConsumedObjectSetsPtr = std::shared_ptr<QueryConsumedObjectSets>;

}
