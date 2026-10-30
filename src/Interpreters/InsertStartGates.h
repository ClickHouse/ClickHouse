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
    /// `BufferSink`, `TimeSeriesSink`, and `DistributedSink` for its local shards) creates the real sinks of
    /// the target table and of its dependent views only during the execution - possibly after the decision
    /// has been made - so their checks could not take part in it (see `addCheck`). Instead, the forwarding
    /// sink registers the checks of the tables behind it while the
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
        /// For a deferred check: the forwarding storage whose sink enforces it. The sinks of a nested INSERT
        /// register the deferred checks of a forwarding storage behind them (a `Buffer` behind an `Alias`)
        /// once more, but possibly after the decision has been made, see `addCheck`.
        std::weak_ptr<const void> enforced_by;
        /// Set for the check of a sink whose errors are ignored - a materialized view branch under
        /// `materialized_views_ignore_errors`. Its failure is stored here and rethrown only by that sink:
        /// the rejection of an ignored branch must not reject a sibling branch writing into the same table
        /// with different limits. Not set together with `deferred`.
        std::shared_ptr<DeferredOutcome> isolated;
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
    /// check now would count those parts. The sink observes the decision that has been made. A deferred
    /// check observes the outcome of the deferred checks of the same forwarding storage that took part in
    /// the decision: the outer sink registers them on behalf of a `Buffer` behind it while the pipeline is
    /// being built, but leaves their enforcement to the `BufferSink` of the nested INSERT, which writes
    /// into the destination only when the buffer is flushed (see `registerHiddenPreWriteChecks`).
    ///
    /// `forwarded` is set when the check arrives through a gate that has been merged into this one: it is
    /// a check of a sink of a query whose decision was pending, see `lent_from`.
    void addCheck(Check check, bool forwarded = false)
    {
        /// The sinks of the group write observe the decision that was lent to it, see `lent_from`.
        if (lent_from && !forwarded)
        {
            lent_from->addCheck(std::move(check));
            return;
        }

        std::unique_lock lock(mutex);

        if (forward_to)
        {
            auto target = forward_to;
            lock.unlock();
            /// A check made directly on a lent gate keeps observing the decision lent to it.
            target->addCheck(std::move(check), forwarded || !lent_from);
            return;
        }

        if (done)
        {
            if (check.deferred && !check.enforced_by.expired())
            {
                for (const auto & [enforced_by, outcome] : failed_deferred)
                {
                    if (sameOwner(enforced_by, check.enforced_by))
                    {
                        check.deferred->exception = outcome->exception;
                        break;
                    }
                }
            }
            return;
        }

        /// A deferred or an isolated check is never deduplicated: its sink waits for its own outcome.
        if (!check.deferred && !check.isolated)
            for (const auto & existing : checks)
                if (!existing.deferred && !existing.isolated
                    && !existing.key.owner_before(check.key) && !check.key.owner_before(existing.key))
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
            /// Reserved before the checks run, so that recording a failure below cannot throw.
            size_t num_deferred = 0;
            for (const auto & check : checks)
                num_deferred += check.deferred != nullptr;
            failed_deferred.reserve(num_deferred);

            try
            {
                for (const auto & check : checks)
                {
                    if (!check.deferred && !check.isolated)
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
                        if (check.deferred)
                        {
                            check.deferred->exception = std::current_exception();
                            failed_deferred.emplace_back(check.enforced_by, check.deferred);
                        }
                        else
                        {
                            check.isolated->exception = std::current_exception();
                        }
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

    /// Whether a deferred check enforced by `storage` failed when the decision was made. Such a gate is not
    /// lent to a group write of `storage`, see `InsertStartGates::get`. An empty `storage` matches any
    /// failed deferred check. The caller holds the lock.
    bool hasFailedDeferredCheckOf(const std::weak_ptr<const void> & storage) const
    {
        const bool any = sameOwner(storage, {});
        for (const auto & [enforced_by, outcome] : failed_deferred)
            if (any || sameOwner(enforced_by, storage))
                return true;
        return false;
    }

    static bool sameOwner(const std::weak_ptr<const void> & left, const std::weak_ptr<const void> & right)
    {
        return !left.owner_before(right) && !right.owner_before(left);
    }

    std::mutex mutex;
    bool done = false;
    std::exception_ptr exception;

    /// Set only for a gate that a group write creates when it borrows the decision of a gate that has already
    /// accepted a write (`lent_from`) while some participants hold gates whose decision is still pending.
    /// The pending gates are merged into this one, so their checks still run - with the limits of their own
    /// queries, before the group write writes anything - instead of being dropped in favour of a decision
    /// made for another query. The checks of the sinks of the group write itself are not run again: they
    /// observe the decision of `lent_from`, as they would if no pending gate had been merged.
    /// Set before the gate is published and never changed, so it is read without the lock.
    std::shared_ptr<InsertStartGate> lent_from;

    /// Registered while the gate is not `done`.
    std::vector<Check> checks;

    /// The deferred checks that failed when the decision was made, with the forwarding storages enforcing
    /// them. Consulted by a deferred check registered after the decision, see `addCheck`.
    std::vector<std::pair<std::weak_ptr<const void>, std::shared_ptr<DeferredOutcome>>> failed_deferred;

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
    ///
    /// `flushed_by_` is the storage whose flush the group write is (the `Buffer`), see `get`.
    InsertStartGates(std::vector<std::shared_ptr<InsertStartGates>> participants_, std::weak_ptr<const void> flushed_by_)
        : participants(std::move(participants_))
        , flushed_by(std::move(flushed_by_))
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
        /// The same holds for a gate whose decision rejected the write through the storage being flushed
        /// (`flushed_by`) - a deferred check enforced by that `Buffer`, with the limits of its destination
        /// write, failed. The group write - e.g. an `OPTIMIZE` or a background flush of that `Buffer` writing
        /// out the rows the query left in it - is not one of the writes the decision accepted, so it decides
        /// anew with its own limits instead of being mistaken for one of them. The failure of a check that
        /// is enforced by another forwarding storage (e.g. an `Alias` over the same table) or of an isolated
        /// check - of a view branch whose errors are ignored - does not prevent the lending: a flush of this
        /// `Buffer` never performs those writes, and the write through this `Buffer` was accepted, so the
        /// flush observes that acceptance instead of counting the parts the earlier flushes of the same
        /// query have created.
        ///
        /// When the chosen gate has made its decision already while other participants hold gates whose
        /// decision is still pending, those are not merged into the chosen gate - their checks would never
        /// run, and a query with stricter limits would be accepted by the decision made for another query.
        /// They are merged into a new gate instead, which runs their checks before the group write writes
        /// anything and lets the sinks of the group write observe the borrowed decision, see
        /// `InsertStartGate::lent_from`.
        ///
        /// The gates are locked at once, in the order of their addresses, which is global as well. A gate
        /// may be shared with registries that are not locked here, so another group write can merge one
        /// of them concurrently; in that case the gates are collected again.
        ///
        /// The gates the registry entries resolve to, following the gates that are already merged into other
        /// ones to their targets: those are the gates that decide. The entries themselves are not rewritten
        /// here: a sink of a query whose decision is pending has to keep reaching the merged gate, see
        /// `InsertStartGate::lent_from`.
        std::vector<InsertStartGatePtr> resolved;
        resolved.reserve(registries.size());
        for (auto * registry : registries)
        {
            auto it = registry->gates.find(table_id);
            resolved.push_back(it == registry->gates.end() ? nullptr : it->second);
        }

        InsertStartGatePtr gate;
        while (true)
        {
            std::vector<InsertStartGatePtr> candidates;
            std::unordered_set<const InsertStartGate *> seen_candidates;
            for (const auto & entry : resolved)
                if (entry && seen_candidates.insert(entry.get()).second)
                    candidates.push_back(entry);

            std::vector<InsertStartGate *> gate_lock_order;
            gate_lock_order.reserve(candidates.size());
            for (const auto & candidate : candidates)
                gate_lock_order.push_back(candidate.get());
            std::sort(gate_lock_order.begin(), gate_lock_order.end());

            std::vector<std::unique_lock<std::mutex>> gate_locks;
            gate_locks.reserve(gate_lock_order.size());
            for (auto * candidate : gate_lock_order)
                gate_locks.emplace_back(candidate->mutex);

            /// A target is locked in the next iteration.
            bool retry = false;
            for (auto & entry : resolved)
            {
                if (entry && entry->forward_to)
                {
                    entry = entry->forward_to;
                    retry = true;
                }
            }
            if (retry)
                continue;

            InsertStartGatePtr chosen;
            for (const auto & candidate : candidates)
            {
                if (candidate->done && !candidate->exception && !candidate->hasFailedDeferredCheckOf(flushed_by))
                {
                    chosen = candidate;
                    break;
                }
            }

            /// A pending lent gate is not chosen: the sinks of the group write would observe the decision lent
            /// to another group write. It is merged like any other pending gate.
            if (!chosen)
            {
                for (const auto & candidate : candidates)
                {
                    if (!candidate->done && !candidate->lent_from)
                    {
                        chosen = candidate;
                        break;
                    }
                }
            }

            /// Everything that can throw is done before the first gate is merged: once a gate forwards to
            /// the chosen one, its checks must have moved there, otherwise a sink still holding the merged
            /// gate would follow `forward_to` and write without its check ever running. An exception thrown
            /// here leaves every gate as it was; the entries added to the registries are empty, which is the
            /// same as no entry.
            std::vector<InsertStartGate *> to_merge;
            to_merge.reserve(candidates.size());
            for (const auto & candidate : candidates)
                if (candidate != chosen && !candidate->done)
                    to_merge.push_back(candidate.get());

            if (!chosen)
            {
                gate = std::make_shared<InsertStartGate>();
            }
            else if (chosen->done && !to_merge.empty())
            {
                /// The decision of `chosen` is borrowed, but the decision of some participants is still
                /// pending: their checks run in a new gate before the group write writes anything.
                gate = std::make_shared<InsertStartGate>();
                gate->lent_from = chosen;
            }
            else
            {
                gate = chosen;
            }

            size_t num_checks = gate->checks.size();
            for (auto * candidate : to_merge)
                num_checks += candidate->checks.size();

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

            /// A participant holding a gate that rejected a write (or a part of it) keeps it - see above - and does not
            /// adopt the new one: the rejection is the pre-write decision of that query, and the write on
            /// behalf of its rows must not be mistaken for a decision the query itself made. The same
            /// holds for a different gate that has already accepted a write of that query. A participant
            /// whose gate has been merged into a lent gate keeps reaching it through the merged gate, so
            /// that the checks of its later sinks take part in the decision.
            for (size_t i = 0; i < entries.size(); ++i)
            {
                auto & existing = *entries[i];
                if (!existing || (!gate->lent_from && resolved[i] && (resolved[i] == gate || resolved[i]->forward_to == gate)))
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

    /// For the registry of a group write: the storage whose flush it is.
    const std::weak_ptr<const void> flushed_by;
};

using InsertStartGatesPtr = std::shared_ptr<InsertStartGates>;

}
