#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block_fwd.h>
#include <Interpreters/InsertStartGates.h>
#include <Interpreters/QueryViewsLog.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/StorageIDMaybeEmpty.h>
#include <QueryPipeline/Chain.h>
#include <Storages/StorageSnapshot.h>

#include <Common/Logger.h>

#include <exception>
#include <memory>
#include <unordered_map>
#include <vector>

namespace DB
{

class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class ViewErrorsRegistry;
using ViewErrorsRegistryPtr = std::shared_ptr<ViewErrorsRegistry>;

class DeduplicationInfo;

struct Settings;

class InsertDependenciesBuilder : public std::enable_shared_from_this<InsertDependenciesBuilder>
{
private:
    friend class ViewErrorsRegistry;

    /// We cannot use std::set, because operator< is inconsistent with operator==
    /// for StorageId and StorageIDPrivate.
    /// Take a look at the detailed comment in StorageID::operator==.
    using StorageIDSet
        = std::unordered_set<StorageIDMaybeEmpty, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

    class DependencyPath
    {
    private:
        std::vector<StorageIDMaybeEmpty> path;
        StorageIDSet visited;

    public:
        void pushBack(StorageIDMaybeEmpty id);
        void popBack();

        [[maybe_unused]] bool empty() const { return path.empty(); }
        const StorageIDMaybeEmpty & back() const { return path.back(); }
        const StorageIDMaybeEmpty & current() const { return back(); }
        StorageIDMaybeEmpty parent(size_t inheritance) const;
        String debugInfo() const;
    };

    using MapIdManyId = std::unordered_map<
        StorageIDMaybeEmpty,
        std::vector<StorageID>,
        StorageID::DatabaseAndTableNameHash,
        StorageID::DatabaseAndTableNameEqual>;
    using MapIdId = std::
        unordered_map<StorageIDMaybeEmpty, StorageIDMaybeEmpty, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdStorage
        = std::unordered_map<StorageIDMaybeEmpty, StoragePtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdMetadata = std::
        unordered_map<StorageIDMaybeEmpty, StorageMetadataPtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

    using MapIdAST
        = std::unordered_map<StorageIDMaybeEmpty, ASTPtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdLock = std::
        unordered_map<StorageIDMaybeEmpty, TableLockHolder, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdContext
        = std::unordered_map<StorageIDMaybeEmpty, ContextPtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdBlock
        = std::unordered_map<StorageIDMaybeEmpty, SharedHeader, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdThreadGroup = std::
        unordered_map<StorageIDMaybeEmpty, ThreadGroupPtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdViewType = std::unordered_map<
        StorageIDMaybeEmpty,
        QueryViewsLogElement::ViewType,
        StorageID::DatabaseAndTableNameHash,
        StorageID::DatabaseAndTableNameEqual>;

public:
    using ConstPtr = std::shared_ptr<const InsertDependenciesBuilder>;

    template <class... Args>
    static ConstPtr create(Args &&... args)
    {
        struct MakeSharedEnabler : public InsertDependenciesBuilder
        {
            explicit MakeSharedEnabler(Args &&... args)
                : InsertDependenciesBuilder(std::forward<Args>(args)...)
            {
            }
        };
        return std::make_shared<const MakeSharedEnabler>(std::forward<Args>(args)...);
    }

    VectorWithMemoryTracking<Chain> createChainWithDependenciesForAllStreams() const;

    Chain createChainWithDependencies() const;
    Chain createChainForDeduplicationRetry(const DeduplicationInfo & info, const std::string & partition_id) const;
    bool isViewsInvolved() const;

    /// Whether the dependent-view branches of this non-parallel quorum insert must be pushed
    /// sequentially: two of them converge on one `ReplicatedMergeTree` table (racing two in-flight
    /// quorum parts of one query against each other), or a hidden write target makes such a
    /// convergence impossible to rule out. See `computeQuorumStreamRequirements`.
    bool quorumRequiresSequentialViews() const { return quorum_sequential_views; }

    void logQueryView(StorageID view_id, std::exception_ptr exception, bool before_start = false) const;
    StorageIDMaybeEmpty getRootViewID() const { return root_view; }

    const auto & getSquashingProcessors() const { return squashing_processors; }

    size_t getSinkStreamSize() const
    {
        return sink_stream_size;
    }

    /// Whether a synchronous insert into `storage` would actually deduplicate blocks, i.e. some sink
    /// consults the deduplication block ids. For MergeTree-family engines this mirrors how
    /// `MergeTreeSink` / `ReplicatedMergeTreeSink` compute their own `deduplicate` flag (an enabled
    /// deduplication window); for storages that forward the write into another table
    /// (`MaterializedView`, `Alias`, proxies) the target is followed, and for storages whose ultimate
    /// target is not cheaply known (`Distributed`, `Buffer`) the probe fails closed. It is used to
    /// decide whether the parallel write fan-out is safe: a per-branch block-number collision only
    /// drops rows when some sink actually deduplicates.
    static bool storageDeduplicatesBlocksOnInsert(const StoragePtr & storage, size_t depth = 0);

    /// Whether writing into `storage` forwards the data through a nested `INSERT` that stamps the
    /// deduplication info from scratch (`Distributed`, `Buffer`, or a forwarding chain ending in one).
    /// Such nested inserts restart the source block numbering per sink branch even when this query
    /// stamps the numbers globally before the fan-out, so the fan-out may produce colliding
    /// deduplication ids for identical blocks regardless of `use_strict_insert_block_limits`. An
    /// `Alias` is looked through instead: its nested `INSERT` (`AliasSink`) runs in this query's
    /// context, receives the chunk's deduplication info intact, and does not restamp a chunk that has
    /// already visited a view, so a globally stamped numbering survives the hop - only a per-branch
    /// numbering (`use_strict_insert_block_limits`), which the callers guard separately, is hazardous
    /// there.
    static bool storageRebuildsDeduplicationIdsOnInsert(const StoragePtr & storage, size_t depth = 0);

    /// Whether inserting into a forwarding `storage` (one for which `storageRebuildsDeduplicationIdsOnInsert`
    /// is true) reaches a table that has a dependent materialized view. The forwarded-to table's dependency
    /// graph lives behind the nested `INSERT` the forwarding sink runs and is invisible to the outer
    /// `InsertDependenciesBuilder` (which only expands the dependencies of the immediate target). When the
    /// parallel write fan-out runs one such nested `INSERT` per branch, each restarts the deduplication
    /// numbering from zero, so identical blocks on different branches collide on any deduplicating dependent
    /// materialized view and rows are silently dropped. This resolves the forwarding chain to the concrete
    /// local target and reports whether it has any dependent view; it fails closed (returns true) when the
    /// ultimate target is not cheaply known here (`Distributed`, `Buffer`, unresolvable, or too deep).
    static bool forwardedInsertReachesDependentView(const StoragePtr & storage, ContextPtr context, size_t depth = 0);

    /// Whether inserting into `storage` can reach a dependent materialized view that is *hidden* from
    /// `collectAllDependencies`. An `Alias` executes a full nested `INSERT` into its target per sink
    /// (`AliasSink`), so the target's dependent-view graph is expanded only inside that nested `INSERT`
    /// at execution time - the outer builder never sees it. A strict insert's per-branch source block
    /// number survives that hop (the chunk has already visited a view, so the nested `INSERT` preserves
    /// its deduplication info instead of restamping it), and a deduplicating view target behind the hop
    /// then sees colliding view-level ids for identical blocks on different branches. Unlike
    /// `forwardedInsertReachesDependentView`, a concrete local target reports false here: its dependent
    /// views are visible to `collectAllDependencies` and are checked directly by the hazard scan. It
    /// fails closed (returns true) when the ultimate target is not cheaply known here (`Distributed`,
    /// `Buffer`, unresolvable, or too deep). Besides the strict-mode deduplication hazard, hidden
    /// views also make the write fan-out bypass `parallel_view_processing = 0` (each branch's nested
    /// `INSERT` pushes the hidden views concurrently), so `InterpreterInsertQuery` also keeps the
    /// insert single-stream when this probe reports true and that setting is disabled.
    static bool forwardedInsertHidesDependentView(const StoragePtr & storage, ContextPtr context, size_t depth = 0);

    /// Whether `storage` has a dependent materialized view which the nested `INSERT` will execute.
    /// This mirrors the pruning done by `collectAllDependencies` for unavailable views, dropped
    /// targets, stale dependency entries, and errors ignored by `materialized_views_ignore_errors`.
    static bool hasExecutableDependentView(const StoragePtr & storage, ContextPtr context);

    /// Whether the INSERT is a non-parallel quorum insert (`insert_quorum >= 2` or `'auto'`, with
    /// `insert_quorum_parallel = 0`). Such an insert permits a single in-flight quorum part per table:
    /// every `ReplicatedMergeTreeSink` checks in `onStart` that the quorum of all previous writes is
    /// already satisfied (`checkQuorumPrecondition`) and throws `UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE`
    /// otherwise. Concurrent sibling sinks of the same query - the write fan-out to
    /// `max_insert_threads` sink chains as well as the branches of dependent materialized views
    /// converging on one target table - would race against the not-yet-satisfied quorum node of the
    /// part committed by a sibling, so such an insert must keep a single sink stream and push its
    /// dependent views sequentially (a sequential sink blocks in `commitPart` until the quorum of its
    /// part is satisfied, so the next sink starts with the quorum node already gone).
    ///
    /// The settings alone do not tell whether a given insert can actually violate that contract: only
    /// writes reaching a `ReplicatedMergeTree` table are quorum writes, and only two of them racing on
    /// the *same* table conflict. `computeQuorumStreamRequirements` derives the two serialization
    /// requirements from the collected sink graph, so a global quorum profile does not cost the write
    /// fan-out of inserts that never reach a replicated table, nor the view parallelism of branches
    /// that write to distinct replicated tables.
    static bool isSequentialQuorumInsert(const Settings & settings);

    /// Whether an insert into `storage` may create a part in a `ReplicatedMergeTree` table: the storage
    /// is one (directly or behind a `MaterializedView` / proxy target chain), or it forwards the write
    /// through a nested `INSERT` whose destination graph is not visible here (a `Distributed` shard, a
    /// `Buffer` flush, a `WindowView` inner table) - in which case the probe fails closed (returns true),
    /// like it does when a target cannot be resolved or the chain is too deep. An `Alias` target is known
    /// locally, so the probe follows it and its hidden dependent-view graph precisely.
    bool storageMayWriteToReplicatedTable(const StoragePtr & storage, size_t depth = 0) const;
    static bool storageMayWriteToReplicatedTable(const StoragePtr & storage, ContextPtr context, size_t depth = 0);

    /// Whether a dependent-view graph hidden behind an `Alias` may write to a `ReplicatedMergeTree`.
    /// It fails closed when a view or its target cannot be resolved.
    bool dependentViewMayWriteToReplicatedTable(const StoragePtr & storage, size_t depth = 0) const;
    static bool dependentViewMayWriteToReplicatedTable(const StoragePtr & storage, ContextPtr context, size_t depth = 0);

    /// Whether a quorum writer reached through `storage` cannot be tied to a concrete target in
    /// `computeQuorumStreamRequirements`. Unlike other forwarding storages, an `Alias` can be
    /// resolved locally, so a direct alias to a replicated table is not hidden.
    bool storageHidesQuorumWriteTarget(const StoragePtr & storage, size_t depth = 0) const;

    /// Returns the physical target id for a visible forwarding chain, so two `Alias` branches
    /// resolving to the same replicated table still participate in the same convergence check.
    StorageIDMaybeEmpty getVisibleQuorumWriteTargetID(const StoragePtr & storage) const;

    /// Whether the physical write target behind `storage` is hidden from this builder: the write is
    /// forwarded through a nested `INSERT` (an `Alias`, a `Distributed` shard, a `Buffer` flush, a
    /// `WindowView` inner table), or a `MaterializedView` / proxy target chain cannot be resolved. Two
    /// branches whose targets are hidden cannot be proven to write to distinct tables, so quorum
    /// convergence checks fail closed on them.
    static bool storageHidesWriteTarget(const StoragePtr & storage, size_t depth = 0);

    /// Whether inserting into `storage` reaches a `Buffer` or a `Distributed`, whose final write runs in a
    /// context other than this query's, so this query's deduplication settings (`deduplicate_insert` /
    /// `insert_deduplicate` / `deduplicate_blocks_in_dependent_materialized_views`) never govern it.
    /// A `Buffer` flushes its accumulated data to the destination through a nested `INSERT` built from the
    /// buffer's *own* context (`StorageBuffer::writeBlockToDestination` copies the buffer's context, not
    /// this query's). A `Distributed` forwards the write to a remote shard whose table is not cheaply known
    /// here and may itself be (or forward to) such a `Buffer`, which would then flush in its own context.
    /// Disabling deduplication for this `INSERT` therefore does not make a parallel write fan-out safe: the
    /// downstream flush can still deduplicate on its destination with the source block numbering restarted
    /// per branch. The forwarding chain (`Alias`, `MaterializedView`, proxies) is followed - those sinks
    /// keep running in this query's context, so it is only a `Buffer` or a `Distributed` at the end of the
    /// chain that switches context; it fails closed (returns true) when a forwarded-to target cannot be
    /// resolved or the chain is too deep.
    static bool storageForwardsInsertToSeparateContext(const StoragePtr & storage, size_t depth = 0);

    /// Whether inserting into `storage` can reach - through a dependent-view graph *hidden* behind an
    /// `Alias` hop - a materialized view whose write forwards into a separate context (a `Buffer` or a
    /// `Distributed`, see `storageForwardsInsertToSeparateContext`). The hidden graph is expanded only
    /// inside the nested `INSERT` each `AliasSink` runs, so neither `collectAllDependencies` nor the
    /// per-entry hazard scan sees it. A parallel write fan-out then runs one such nested `INSERT` per
    /// branch; the separate-context sink at the end of the hidden chain (`BufferSink` /
    /// `DistributedSink`) drops the carried deduplication info and its downstream write restamps the
    /// source block numbering from scratch, per branch - so identical blocks on different branches can
    /// collide on the final deduplicating destination and rows are silently dropped, regardless of this
    /// query's deduplication settings (they do not reach the separate-context write). It fails closed
    /// (returns true) when a hop of the chain cannot be resolved or the chain is too deep.
    static bool forwardedInsertHidesDependentViewForwardingToSeparateContext(
        const StoragePtr & storage, ContextPtr context, size_t depth = 0);

    /// Whether the dependent-view graph of `storage` itself (not hidden behind a forwarding hop)
    /// contains a materialized view whose write forwards into a separate context, at any depth -
    /// including further view chains and view graphs hidden behind `Alias` targets. Helper for
    /// `forwardedInsertHidesDependentViewForwardingToSeparateContext`; fails closed (returns true)
    /// when a view or its target cannot be resolved or the graph is too deep.
    static bool dependentViewForwardsInsertToSeparateContext(
        const StoragePtr & storage, ContextPtr context, size_t depth = 0);

    size_t getViewProcessingNumThreads() const;


protected:
    InsertDependenciesBuilder(
        StoragePtr table,
        ASTPtr query,
        SharedHeader insert_header,
        bool async_insert_,
        bool skip_destination_table_,
        size_t max_insert_threads,
        ContextPtr context,
        InsertStartGatesPtr insert_start_gates_ = nullptr);

private:
    bool isView(StorageIDMaybeEmpty id) const;

    std::pair<ContextPtr, ContextPtr> createSelectInsertContext(const DependencyPath & path);
    bool observePath(const DependencyPath & path);
    String debugTree() const;
    String debugPath(const DependencyPath & path) const;
    void collectAllDependencies();

    /// Derives `quorum_single_stream` and `quorum_sequential_views` from the sink graph collected by
    /// `collectAllDependencies` for a non-parallel quorum insert (see `isSequentialQuorumInsert`).
    void computeQuorumStreamRequirements();

    Chain createPreSink(StorageIDMaybeEmpty view_id) const;
    Chain createSelect(StorageIDMaybeEmpty view_id) const;
    Chain createSink(StorageIDMaybeEmpty view_id) const;
    Chain createSinkImpl(StorageIDMaybeEmpty view_id) const;
    Chain createPostSink(StorageIDMaybeEmpty view_id) const;

    Chain createRetry(const std::vector<StorageIDMaybeEmpty> & path, StorageIDMaybeEmpty start_from, const std::string & partition) const;

    static QueryViewsLogElement::ViewStatus getQueryViewStatus(std::exception_ptr exception, bool before_start);

    String getViewQueryForLog(StorageID view_id) const;

    StorageIDMaybeEmpty init_table_id;
    StoragePtr init_storage;
    ASTPtr init_query;
    SharedHeader init_header;
    ContextPtr init_context;

    bool async_insert = false;
    bool skip_destination_table = false;
    bool sequential_quorum_insert = false;
    /// Graph-derived quorum serialization requirements, see `computeQuorumStreamRequirements`.
    bool quorum_single_stream = false;
    bool quorum_sequential_views = false;
    size_t sink_stream_size = 1;

    /// When the insertion is made into a materialized view, the root_view is the view itself and dependent_views contains its inner table.
    /// When the insertion is made into a regular table (it is init_table_id), the root_view is {} / StorageID::createEmpty() and dependent_views contains init_table_id.
    StorageIDMaybeEmpty root_view;

    MapIdManyId dependent_views;
    MapIdId inner_tables;
    MapIdId source_tables;
    MapIdStorage storages;
    MapIdViewType view_types;
    MapIdLock storage_locks;
    MapIdMetadata metadata_snapshots;
    MapIdAST select_queries;
    MapIdContext insert_contexts;
    MapIdContext select_contexts;
    MapIdBlock input_headers;
    MapIdBlock output_headers;
    MapIdThreadGroup thread_groups;
    /// The gates of this query's destination tables, shared by the sinks of all the parallel streams
    /// writing into them - including the sinks a forwarding destination (an `Alias`) creates inside
    /// its nested INSERT, which receives this registry instead of creating its own.
    InsertStartGatesPtr insert_start_gates;

    using SquashingProcessorsMap = std::unordered_map<
        StorageIDMaybeEmpty,
        std::vector<std::list<ProcessorPtr>::const_iterator>,
        StorageID::DatabaseAndTableNameHash,
        StorageID::DatabaseAndTableNameEqual>;

    mutable SquashingProcessorsMap squashing_processors;

    ViewErrorsRegistryPtr views_error_registry;

    LoggerPtr logger;

public:
    // expose settings value into public
    bool deduplicate_blocks = false;
    bool deduplicate_blocks_in_dependent_materialized_views = false;
    bool insert_null_as_default = false;
    bool materialized_views_ignore_errors = false;
    bool squash_parallel_inserts = false;
    bool ignore_materialized_views_with_dropped_target_table = false;
};

}
