#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

class StorageView final : public StorageWithCommonVirtualColumns
{
    static VirtualColumnsDescription createVirtuals();

public:
    StorageView(
        const StorageID & table_id_,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        const String & comment,
        bool is_parameterized_view_ = false);

    std::string getName() const override { return "View"; }
    bool isView() const override { return true; }
    bool supportsTruncate() const override { return false; }
    bool isParameterizedView() const { return is_parameterized_view; }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    /// `readImpl` forwards the requested column NAMES into the inner query, whose column types may
    /// differ from the view's declared ones, so a rewritten `arr.size0` may not resolve there.
    bool supportsOptimizationToSubcolumns() const override { return false; }
    bool supportsColumnsWithDynamicStructure() const override { return true; }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;

    StoragePtr getUnderlyingMergeTreeStorageForParallelReplicas(const ContextPtr & context) const;

    /// If this is a trivial view over a Distributed table, returns the underlying StorageDistributed.
    /// Returns nullptr otherwise.
    StoragePtr tryGetUnderlyingDistributed(const StorageSnapshotPtr & snapshot, ContextPtr context) const;

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void drop() override;
    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    static void replaceQueryParametersIfParameterizedView(ASTPtr & outer_query, const NameToNameMap & parameter_values);

    static void replaceWithSubquery(ASTSelectQuery & select_query, ASTPtr & view_name, const StorageMetadataPtr & metadata_snapshot, const bool parameterized_view)
    {
        replaceWithSubquery(select_query, metadata_snapshot->getSelectQuery().inner_query->clone(), view_name, parameterized_view);
    }

    static void replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name, bool parameterized_view);
    static ASTPtr restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name);

    static ContextPtr getViewSubqueryContext(ContextPtr context, const StorageSnapshotPtr & storage_snapshot);

    /// Whether the view's inner query runs as somebody other than the invoker, so that the rows it
    /// filters out are rows the invoker has no right to observe. Such a view must not be inlined
    /// into the invoker's query, and expressions from the invoker's query must not be evaluated
    /// below its own filtering. See `IQueryPlanStep::isSecurityBarrier`.
    static bool isSecurityBarrier(const StorageInMemoryMetadata & metadata, const ContextPtr & context);

    /// Whether `additional_table_filters` has a predicate that applies to this view. Such a
    /// predicate is evaluated in the view's output namespace and can hide rows just like a row
    /// policy attached to the view.
    static bool hasAdditionalTableFilter(const StorageID & storage_id, const String & alias, const ContextPtr & context);

    /// Whether the view's inner query can drop or collapse rows at all. `false` is returned only
    /// when the query provably preserves every row of a plainly readable source, so that a
    /// projection-only view keeps the fully optimizable path even when `isSecurityBarrier` holds;
    /// anything unproven counts as able to hide rows. The plan-level marking stays exact either
    /// way — `readImpl` marks only the steps that actually drop rows.
    /// `remote_source_is_read_identically` relaxes the fail-closed treatment of a remote source
    /// table for the one caller whose alternative reads that same source in exactly the same way -
    /// the trivial-view pushdown to `Distributed`. There, whatever the shards resolve the table to
    /// (possibly a filtering view of their own) is read through the same `StorageDistributed::read`
    /// with or without the rewrite, and a barrier below it is enforced by the shard's own planner,
    /// so the remote source is not a reason to give up on the view itself. It is deliberately not
    /// propagated into nested subqueries.
    static bool canHideRows(const ASTPtr & inner_query, const ContextPtr & context, bool remote_source_is_read_identically = false);

protected:
    bool is_parameterized_view;
};

}
