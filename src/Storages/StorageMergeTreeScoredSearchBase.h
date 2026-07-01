#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/SelectQueryInfo.h>

#include <memory>
#include <optional>
#include <utility>

namespace DB
{

class IScorer;

/// Base for the scored-search table-function storages
/// (`vectorSearch`, and future `textSearch` / `hybridSearch`).
class StorageMergeTreeScoredSearchBase : public IStorage
{
public:
    StorageMergeTreeScoredSearchBase(
        const StorageID & table_id_,
        StoragePtr source_table_,
        const ColumnsDescription & columns,
        const std::vector<MergeTreeIndexPtr> & scorer_indexes_);

    /// Check if the source columns contain reserved names for scored-search virtual columns.
    static void checkSourceColumnsForReservedNames(const ColumnsDescription & source_columns, const StorageID & source_storage_id);

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processing_stage,
        size_t max_block_size,
        size_t num_streams) final;

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info) const final;

    const MergeTreeData * getSourceTable() const { return dynamic_cast<const MergeTreeData *>(source_table.get()); }
    const std::vector<MergeTreeIndexPtr> & getScorerIndexes() const { return scorer_indexes; }
    /// Constructs the scorer for the search query.
    virtual std::shared_ptr<IScorer> createScorer() const = 0;

protected:
    /// Owns the source storage.
    StoragePtr source_table;
    /// Indexes used by scorer in the search query.
    std::vector<MergeTreeIndexPtr> scorer_indexes;

private:
    /// Compile the source table's SELECT row policy into a filter DAG
    /// (nullopt when no policy applies), paired with the sets the policy references.
    static std::pair<std::optional<FilterDAGInfo>, PreparedSetsPtr> buildRowPolicyFilterInfo(
        ContextPtr context,
        const ColumnsDescription & source_columns,
        const StorageID & source_storage_id);

    PartitionIdToMaxBlockPtr getMaxBlockNumbersToRead(ContextPtr context) const;
};

}
