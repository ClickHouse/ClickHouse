#pragma once

#include <Interpreters/StorageIDMaybeEmpty.h>

#include <algorithm>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace DB
{

/// One gate per destination table of an INSERT query, shared by all the sinks that write into that
/// table on behalf of the query. With `max_insert_threads` greater than one the query writes through
/// several sinks running in parallel, and their `onStart` calls are not ordered with respect to each
/// other's writes. See `SinkToStorage::runOnceBeforeFirstWrite`.
///
/// The gate remembers the outcome of the check, including a failure: the first sink to arrive runs it
/// while the others wait, and every one of them observes that same outcome. Rerunning the check after
/// a failure - what `std::call_once` does - would not be equivalent, because the check looks at mutable
/// state (the parts of the destination table), so a concurrent merge or cleanup could let one sink of
/// the query be rejected while another one, running the check a moment later, proceeds.
class InsertStartGate
{
public:
    void run(const std::function<void()> & check)
    {
        std::lock_guard lock(mutex);

        if (!done)
        {
            try
            {
                check();
            }
            catch (...)
            {
                exception = std::current_exception();
            }
            done = true;
        }

        if (exception)
            std::rethrow_exception(exception);
    }

private:
    std::mutex mutex;
    bool done = false;
    std::exception_ptr exception;
};

using InsertStartGatePtr = std::shared_ptr<InsertStartGate>;

/// The per-query registry the gates come from. `InsertDependenciesBuilder` creates one for the INSERT
/// query and takes the gate of every destination table it reaches from it, so all the sinks of the
/// query writing into the same physical table - the parallel streams of the destination table itself
/// as well as the branches of different materialized views converging on one target table - share one
/// gate. A destination that forwards the write through a nested INSERT running in this query's
/// context (an `Alias`) hands the registry over to that nested INSERT, so the sinks created there
/// keep sharing the gates of the outer query: otherwise every branch of the outer fan-out would
/// create its own gates and the pre-write checks would not be shared across the branches.
class InsertStartGates
{
public:
    InsertStartGates() = default;

    /// A registry for a write performed on behalf of several queries at once: a `Buffer` flush writes
    /// out a block that may hold rows buffered by different queries, and its pre-write decision has to
    /// be observed by every one of them - a query whose rows were written by a flush someone else
    /// triggered must not re-run the check later and count the parts that write created from its own
    /// rows. Every gate this registry hands out is therefore shared with all the participating
    /// queries' registries. A participant that already holds a gate for the table lends it to the
    /// group instead: that query has already made (or is making) its pre-write decision for the table
    /// - e.g. an earlier flush or direct write of the same query - and the group write must observe
    /// that decision rather than re-enter the check and count the parts of that earlier write.
    explicit InsertStartGates(std::vector<std::shared_ptr<InsertStartGates>> participants_)
        : participants(std::move(participants_))
    {
    }

    InsertStartGatePtr get(const StorageIDMaybeEmpty & table_id)
    {
        if (participants.empty())
        {
            std::lock_guard lock(mutex);
            auto & gate = gates[table_id];
            if (!gate)
                gate = std::make_shared<InsertStartGate>();
            return gate;
        }

        /// A group write hands out one and the same gate object to this registry and to every
        /// participant, so looking the gate up, creating it and sharing it has to be one atomic step.
        /// Otherwise two group writes over the same participants - the flushes of the several shards of
        /// one `Buffer`, which run in parallel in `flush_pool` - or a group write racing a direct write
        /// of a participating query could each find no gate for the table and hand out different gates
        /// to the sinks of one query, which is exactly what the gates are there to prevent.
        ///
        /// All the registries involved are locked at once, in the order of their addresses. That order
        /// is global, so a registry taking part in several group writes cannot deadlock against them.
        std::vector<InsertStartGates *> registries;
        registries.reserve(participants.size() + 1);
        registries.push_back(this);
        for (const auto & participant : participants)
            registries.push_back(participant.get());

        std::vector<InsertStartGates *> lock_order = registries;
        std::sort(lock_order.begin(), lock_order.end());
        lock_order.erase(std::unique(lock_order.begin(), lock_order.end()), lock_order.end());

        std::vector<std::unique_lock<std::mutex>> locks;
        locks.reserve(lock_order.size());
        for (auto * registry : lock_order)
            locks.emplace_back(registry->mutex);

        /// A participant that already holds a gate for the table lends it to the group: that query has
        /// already made (or is making) its pre-write decision for the table, and this write must
        /// observe that decision instead of re-entering the check.
        InsertStartGatePtr gate;
        for (auto * registry : registries)
        {
            auto it = registry->gates.find(table_id);
            if (it != registry->gates.end() && it->second)
            {
                gate = it->second;
                break;
            }
        }

        if (!gate)
            gate = std::make_shared<InsertStartGate>();

        for (auto * registry : registries)
        {
            auto & existing = registry->gates[table_id];
            if (!existing)
                existing = gate;
        }

        return gate;
    }

private:
    std::mutex mutex;
    std::unordered_map<StorageIDMaybeEmpty, InsertStartGatePtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>
        gates;

    /// Non-empty only for the registry of a group write (see the constructor above).
    const std::vector<std::shared_ptr<InsertStartGates>> participants;
};

using InsertStartGatesPtr = std::shared_ptr<InsertStartGates>;

}
