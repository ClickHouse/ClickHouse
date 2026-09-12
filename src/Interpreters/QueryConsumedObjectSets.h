#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
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

    /// Called when a read of `table_uuid` installs the capture, before it consumes its first object.
    /// It creates an empty captured set, so that "the read consumed no object at all" is distinct from
    /// "nothing was captured for this table". Without it a read that consumes zero objects would make
    /// `getModificationHash` fall back to a fresh listing at finalization, which reopens the listing
    /// race in the `A -> {} -> A` direction: the pre-read hash lists `A`, the read sees no object at
    /// all, and the relist reproduces `A`, so a result produced from no data is stored under the key of
    /// the object set `A`. With the empty set captured, the two hashes differ and the entry is dropped.
    void beginCapture(const UUID & table_uuid);

    /// Called (possibly concurrently from several read streams) for every object the read consumes.
    void add(const UUID & table_uuid, Object object);

    /// Called when a read of `table_uuid` prunes the object set (e.g. a `_path`/`_file` or
    /// Hive-partition filter narrows the iterator result): the consumed set is then a filtered subset
    /// of the full listing the pre-read hash was built from, so the pre/post hashes could never
    /// compare equal for an unchanged table. `getModificationHash` fails closed for such a table
    /// instead of comparing incomparable sets. Sticky for the whole query, even if another read of the
    /// same table does not prune.
    void markPruned(const UUID & table_uuid);

    /// Whether any read of `table_uuid` in this query pruned the object set.
    bool isPruned(const UUID & table_uuid) const;

    /// The objects consumed for `table_uuid`, or nullopt if no read of it installed a capture (e.g. the
    /// table was not read, or this is the pre-read check that runs before the plan was built). A read
    /// that consumed no object returns an empty vector, not nullopt - see `beginCapture`.
    std::optional<std::vector<Object>> get(const UUID & table_uuid) const;

private:
    mutable std::mutex mutex;
    std::map<UUID, std::vector<Object>> objects_by_table;
    std::set<UUID> pruned_tables;
};

using QueryConsumedObjectSetsPtr = std::shared_ptr<QueryConsumedObjectSets>;

}
