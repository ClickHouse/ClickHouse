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
        std::unique_lock lock(mutex);

        /// The gate was merged into another one by a group write before any sink ran the check through it,
        /// see `InsertStartGates::get`. The decision is made (or has been made) by that gate. The lock of this
        /// gate is released first, so that no thread ever holds the locks of two gates while running a check.
        if (forward_to)
        {
            auto target = forward_to;
            lock.unlock();
            target->run(check);
            return;
        }

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
    friend class InsertStartGates;

    std::mutex mutex;
    bool done = false;
    std::exception_ptr exception;

    /// Set only while the gate is not `done`: from then on every sink holding this gate observes the
    /// decision of `forward_to` instead. Never changes once set.
    std::shared_ptr<InsertStartGate> forward_to;
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
    /// A participant may itself be the registry of a group write - `Buffer` tables can be chained -
    /// in which case the queries behind it participate as well, see `collectRegistries`.
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
        collectRegistries(registries);

        std::vector<InsertStartGates *> lock_order = registries;
        std::sort(lock_order.begin(), lock_order.end());
        lock_order.erase(std::unique(lock_order.begin(), lock_order.end()), lock_order.end());

        std::vector<std::unique_lock<std::mutex>> locks;
        locks.reserve(lock_order.size());
        for (auto * registry : lock_order)
            locks.emplace_back(registry->mutex);

        /// The group write has already handed out a gate for the table, so its own later sinks - e.g. the
        /// parallel branches created during the write - observe the same decision, a rejection included.
        if (auto it = gates.find(table_id); it != gates.end() && it->second)
            return it->second;

        /// A participant that already holds a gate for the table lends it to the group: that query has
        /// already made (or is making) its pre-write decision for the table, and this write must
        /// observe that decision instead of re-entering the check. A decision that was already made
        /// takes precedence over a pending one.
        ///
        /// Several participants may hold different gates, and the sinks of those queries may already
        /// hold them while their `onStart` has not run yet: `InsertDependenciesBuilder` hands the gates
        /// out while building the pipeline. Replacing the registry entries would not reach these sinks,
        /// so every other pending gate is merged into the chosen one instead - its sinks observe the
        /// decision of the chosen gate. Otherwise a query whose rows are written by this group write could
        /// later run the check again through its own pending gate and count the parts this write created.
        ///
        /// A gate that rejected a write is not lent, though. The write it rejected did not happen, and
        /// its rows are still in the buffer, waiting to be written by a later flush - the one asking
        /// here. Sharing the rejection with that flush would make it fail without looking at the table
        /// again, and the rows would stay in the buffer for good, however far the parts have been
        /// merged away or the limit has been raised since; a query that only shares the flush with them
        /// would be rejected as well. The rejection stays with the query it was made for: its own sinks
        /// keep observing it through their registry, while a write on behalf of its buffered rows is a
        /// new write that decides anew.
        ///
        /// The gates are locked at once, in the order of their addresses, which is global as well. A gate
        /// may be shared with registries that are not locked here, so another group write can merge one
        /// of them concurrently; in that case the gates are collected again.
        InsertStartGatePtr gate;
        while (true)
        {
            std::vector<InsertStartGatePtr> candidates;
            for (auto * registry : registries)
            {
                auto it = registry->gates.find(table_id);
                if (it == registry->gates.end() || !it->second)
                    continue;
                if (std::find(candidates.begin(), candidates.end(), it->second) == candidates.end())
                    candidates.push_back(it->second);
            }

            std::vector<InsertStartGate *> gate_lock_order;
            gate_lock_order.reserve(candidates.size());
            for (const auto & candidate : candidates)
                gate_lock_order.push_back(candidate.get());
            std::sort(gate_lock_order.begin(), gate_lock_order.end());

            std::vector<std::unique_lock<std::mutex>> gate_locks;
            gate_locks.reserve(gate_lock_order.size());
            for (auto * candidate : gate_lock_order)
                gate_locks.emplace_back(candidate->mutex);

            /// Follow the gates that are already merged into other ones to their targets: those are the
            /// gates that decide. A target is locked in the next iteration.
            bool retry = false;
            for (auto & candidate : candidates)
            {
                if (candidate->forward_to)
                {
                    InsertStartGatePtr target = candidate->forward_to;
                    for (auto * registry : registries)
                    {
                        auto it = registry->gates.find(table_id);
                        if (it != registry->gates.end() && it->second == candidate)
                            it->second = target;
                    }
                    retry = true;
                }
            }
            if (retry)
                continue;

            for (const auto & candidate : candidates)
            {
                if (candidate->done && !candidate->exception)
                {
                    gate = candidate;
                    break;
                }
            }

            if (!gate)
            {
                for (const auto & candidate : candidates)
                {
                    if (!candidate->done)
                    {
                        gate = candidate;
                        break;
                    }
                }
            }

            if (!gate)
                gate = std::make_shared<InsertStartGate>();

            std::vector<InsertStartGatePtr> merged;
            for (const auto & candidate : candidates)
            {
                if (candidate != gate && !candidate->done)
                {
                    candidate->forward_to = gate;
                    merged.push_back(candidate);
                }
            }

            /// A participant holding a gate that rejected a write keeps it - see above - and does not
            /// adopt the new one: the rejection is the pre-write decision of that query, and the write on
            /// behalf of its rows must not be mistaken for a decision the query itself made. The same
            /// holds for a different gate that has already accepted a write of that query.
            for (auto * registry : registries)
            {
                auto & existing = registry->gates[table_id];
                if (!existing || std::find(merged.begin(), merged.end(), existing) != merged.end())
                    existing = gate;
            }

            break;
        }

        return gate;
    }

private:
    /// This registry and, transitively, the registries of every query it writes on behalf of.
    ///
    /// The participants of a group write are not necessarily the registries of the queries themselves:
    /// a `Buffer` flush hands its group registry over to the write into the destination table, and when
    /// that destination is another `Buffer` the block is buffered there together with that group
    /// registry, which the second buffer then lists as a single participant of its own later flush. The
    /// queries behind the wrapper have to observe the write into the final destination just as the
    /// direct participants do, so the whole participant graph is walked here instead of its first level
    /// only. The graph is acyclic - a group registry lists only registries that already existed when it
    /// was constructed - and `participants` is fixed at construction, so no lock is needed to walk it.
    void collectRegistries(std::vector<InsertStartGates *> & registries)
    {
        if (std::find(registries.begin(), registries.end(), this) != registries.end())
            return;

        registries.push_back(this);

        for (const auto & participant : participants)
            participant->collectRegistries(registries);
    }

    std::mutex mutex;
    std::unordered_map<StorageIDMaybeEmpty, InsertStartGatePtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>
        gates;

    /// Non-empty only for the registry of a group write (see the constructor above).
    const std::vector<std::shared_ptr<InsertStartGates>> participants;
};

using InsertStartGatesPtr = std::shared_ptr<InsertStartGates>;

}
