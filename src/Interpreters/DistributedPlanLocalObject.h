#pragma once

#include <base/types.h>

#include <atomic>
#include <mutex>
#include <optional>

namespace DB
{

/// The first object that a query resolved by name while it was analyzed and that exists only on the initiator today:
/// a dictionary, the embedded dictionaries or a `Join` table. A worker of `make_distributed_plan` shares the table data
/// but not these objects, and one of them is enough to run the query locally, so only the first is kept: the resolvers
/// run at execution as well, once per block for `dictGet`, and every call after the first costs a single load of the
/// flag. Written by the resolvers through `Context::addDistributedPlanLocalObject`, read by the fallback decision.
/// Analysis and planning of a query normally run on one thread, but a materialized view analyzes its `SELECT` per
/// inserted block on the insert's pipeline threads, in parallel under `parallel_view_processing`, so a read can meet the
/// first write; both sides take the mutex.
struct DistributedPlanLocalObject
{
    enum class Kind : UInt8
    {
        Dictionary,
        JoinTable,
        EmbeddedDictionaries,
    };

    struct Entry
    {
        Kind kind;
        String name;
    };

    /// Stores the first call's object; later calls are no-ops.
    void add(Kind kind, const String & name);

    /// The recorded object, or nothing.
    std::optional<Entry> get() const;

    static std::string_view kindName(Kind kind);

private:
    /// Set under the mutex after `entry`; a stale `false` only sends a caller to the mutex, where `entry` decides.
    std::atomic<bool> recorded{false};
    mutable std::mutex mutex;
    std::optional<Entry> entry;
};

using DistributedPlanLocalObjectPtr = std::shared_ptr<DistributedPlanLocalObject>;

}
