#pragma once

#include <Interpreters/StorageIDMaybeEmpty.h>

#include <algorithm>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

/// One gate per destination table of an INSERT query, shared by all the sinks that write into that
/// table on behalf of the query. With `max_insert_threads` greater than one the query writes through
/// several sinks running in parallel, and their `onStart` calls are not ordered with respect to each
/// other's writes. See `SinkToStorage::runPreWriteCheck`.
///
/// The gate remembers the outcome of the check, including a failure: the first sink to arrive runs the
/// checks of all the sinks holding the gate while the others wait, and every one of them observes that
/// same outcome. Rerunning the check after
/// a failure - what `std::call_once` does - would not be equivalent, because the check looks at mutable
/// state (the parts of the destination table), so a concurrent merge or cleanup could let one sink of
/// the query be rejected while another one, running the check a moment later, proceeds.
class InsertStartGate
{
public:
    /// The outcome of a deferred check. A sink that forwards the write through a nested INSERT (`AliasSink`,
    /// `BufferSink`, `TimeSeriesSink`) creates the real sinks of the target table only during the execution -
    /// possibly after the decision has been made - so their checks could not take part in it (see
    /// `addCheck`). Instead, the forwarding sink registers the checks of the tables behind it while the
    /// pipeline is being built, as deferred checks. They run together with the other checks, before anything
    /// is written, but their failure does not reject the query on its own: it is stored here, and the
    /// forwarding sink rethrows it before it actually writes through the nested INSERT. A `Buffer` may only
    /// keep the rows in memory, in which case the limits of its destination must not reject the query.
    struct DeferredOutcome
    {
        std::exception_ptr exception;
    };

    /// The pre-write check of one sink. `key` identifies the context the check runs with: the checks of
    /// the parallel sinks of one stream are the same and are run only once. `check` must not keep the
    /// sink alive - the gate may outlive it, see `addCheck`.
    struct Check
    {
        std::weak_ptr<const void> key;
        std::function<void()> check;
        /// Set for the check of a write that is hidden behind a forwarding sink, see `DeferredOutcome`.
        std::shared_ptr<DeferredOutcome> deferred;
    };

    /// `InsertStartGates::get` moves the checks of a merged gate without allocating, so it cannot fail half way.
    static_assert(std::is_nothrow_move_constructible_v<Check>);

    /// Registers the pre-write check of a sink holding the gate. The sinks get their gates while the
    /// pipeline is being built, before any of them writes, and the first sink to start runs the checks of
    /// all of them. The sinks writing into one table do not necessarily run the same check: the limits
    /// come from the context of the sink, and the branches of different materialized views, or a write
    /// through a `Buffer`, may run with different settings. The decision must not depend on which sink
    /// happens to start first, so a sink with stricter limits rejects the query even if a sink with
    /// looser ones has started before it.
    ///
    /// A check registered after the decision has been made - by a sink created during the execution, in
    /// a nested `INSERT` - is not run: the query may already have written into the table, and running the
    /// check now would count those parts. The sink observes the decision that has been made.
    void addCheck(Check check)
    {
        std::unique_lock lock(mutex);

        if (forward_to)
        {
            auto target = forward_to;
            lock.unlock();
            target->addCheck(std::move(check));
            return;
        }

        if (done)
            return;

        /// A deferred check is never deduplicated: its forwarding sink waits for its own outcome.
        if (!check.deferred)
            for (const auto & existing : checks)
                if (!existing.deferred && !existing.key.owner_before(check.key) && !check.key.owner_before(existing.key))
                    return;

        checks.push_back(std::move(check));
    }

    void run()
    {
        std::unique_lock lock(mutex);

        /// The gate was merged into another one by a group write before any sink ran the check through it,
        /// see `InsertStartGates::get`. The decision is made (or has been made) by that gate. The lock of this
        /// gate is released first, so that no thread ever holds the locks of two gates while running a check.
        if (forward_to)
        {
            auto target = forward_to;
            lock.unlock();
            target->run();
            return;
        }

        if (!done)
        {
            try
            {
                for (const auto & check : checks)
                {
                    if (!check.deferred)
                    {
                        check.check();
                        continue;
                    }

                    try
                    {
                        check.check();
                    }
                    catch (...)
                    {
                        check.deferred->exception = std::current_exception();
                    }
                }
            }
            catch (...)
            {
                exception = std::current_exception();
            }
            done = true;
            checks.clear();
        }

        if (exception)
            std::rethrow_exception(exception);
    }

private:
    friend class InsertStartGates;

    std::mutex mutex;
    bool done = false;
    std::exception_ptr exception;

    /// Registered while the gate is not `done`.
    std::vector<Check> checks;

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

        /// The group write has already handed out a gate for the table, so its own later sinks - e.g. the
        /// parallel branches created during the write - observe the same decision, a rejection included.
        /// An entry is never removed; another group write may only replace it with the gate it was merged
        /// into, which makes the same decision. So there is no need to walk and lock the participants.
        {
            std::lock_guard lock(mutex);
            if (auto it = gates.find(table_id); it != gates.end() && it->second)
                return it->second;
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
        std::unordered_set<const InsertStartGates *> visited;
        collectRegistries(registries, visited);

        std::vector<InsertStartGates *> lock_order = registries;
        std::sort(lock_order.begin(), lock_order.end());

        std::vector<std::unique_lock<std::mutex>> locks;
        locks.reserve(lock_order.size());
        for (auto * registry : lock_order)
            locks.emplace_back(registry->mutex);

        /// Checked again under the locks: another sink of this group write may have been faster.
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
            std::unordered_set<const InsertStartGate *> seen_candidates;
            for (auto * registry : registries)
            {
                auto it = registry->gates.find(table_id);
                if (it == registry->gates.end() || !it->second)
                    continue;
                if (seen_candidates.insert(it->second.get()).second)
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
            for (auto * registry : registries)
            {
                auto it = registry->gates.find(table_id);
                if (it != registry->gates.end() && it->second && it->second->forward_to)
                {
                    it->second = it->second->forward_to;
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

            /// Everything that can throw is done before the first gate is merged: once a gate forwards to
            /// the chosen one, its checks must have moved there, otherwise a sink still holding the merged
            /// gate would follow `forward_to` and write without its check ever running. An exception thrown
            /// here leaves every gate as it was; the entries added to the registries are empty, which is the
            /// same as no entry.
            std::vector<InsertStartGate *> to_merge;
            to_merge.reserve(candidates.size());
            size_t num_checks = gate->checks.size();
            for (const auto & candidate : candidates)
            {
                if (candidate != gate && !candidate->done)
                {
                    to_merge.push_back(candidate.get());
                    num_checks += candidate->checks.size();
                }
            }

            if (!gate->done)
                gate->checks.reserve(num_checks);

            std::vector<InsertStartGatePtr *> entries;
            entries.reserve(registries.size());
            for (auto * registry : registries)
                entries.push_back(&registry->gates[table_id]);

            /// Nothing below throws: the checks are moved into the reserved capacity.
            for (auto * candidate : to_merge)
            {
                candidate->forward_to = gate;
                /// The checks of the sinks holding the merged gate now take part in the decision of
                /// the chosen gate, unless it has been made already (see `InsertStartGate::addCheck`).
                if (!gate->done)
                    for (auto & check : candidate->checks)
                        gate->checks.push_back(std::move(check));
                candidate->checks.clear();
            }

            /// A participant holding a gate that rejected a write keeps it - see above - and does not
            /// adopt the new one: the rejection is the pre-write decision of that query, and the write on
            /// behalf of its rows must not be mistaken for a decision the query itself made. The same
            /// holds for a different gate that has already accepted a write of that query. Every gate in
            /// the registries is a locked candidate, so the gates forwarding to the chosen one are exactly
            /// the ones merged above.
            for (auto * existing : entries)
            {
                if (!*existing || (*existing)->forward_to == gate)
                    *existing = gate;
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
    ///
    /// `visited` keeps the walk linear in the size of the graph: a flush of a `Buffer` shard may list many
    /// thousands of participating queries.
    void collectRegistries(std::vector<InsertStartGates *> & registries, std::unordered_set<const InsertStartGates *> & visited)
    {
        if (!visited.insert(this).second)
            return;

        registries.push_back(this);

        for (const auto & participant : participants)
            participant->collectRegistries(registries, visited);
    }

    /// The gates are keyed by the physical table incarnation, not by its name. A `Buffer` retains the
    /// registries of the queries whose rows it holds after those queries have finished and resolves its
    /// destination by name again on every flush, so the table behind a name may have been dropped and
    /// recreated in between: the new table must receive its own pre-write decision instead of reusing
    /// the one made for the old table. A table without a UUID (an `Ordinary` database) is keyed by name.
    struct PhysicalTableHash
    {
        size_t operator()(const StorageID & table_id) const
        {
            if (table_id.hasUUID())
            {
                const auto & items = table_id.uuid.toUnderType().items;
                return items[0] ^ items[1]; /// A UUID is random already.
            }
            return StorageID::DatabaseAndTableNameHash()(table_id);
        }
    };

    struct PhysicalTableEqual
    {
        bool operator()(const StorageID & left, const StorageID & right) const
        {
            if (left.hasUUID() || right.hasUUID())
                return left.uuid == right.uuid;
            return StorageID::DatabaseAndTableNameEqual()(left, right);
        }
    };

    std::mutex mutex;
    std::unordered_map<StorageIDMaybeEmpty, InsertStartGatePtr, PhysicalTableHash, PhysicalTableEqual> gates;

    /// Non-empty only for the registry of a group write (see the constructor above).
    const std::vector<std::shared_ptr<InsertStartGates>> participants;
};

using InsertStartGatesPtr = std::shared_ptr<InsertStartGates>;

}
