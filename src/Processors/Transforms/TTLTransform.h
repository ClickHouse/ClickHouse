#pragma once

#include <Interpreters/PreparedSets.h>
#include <Processors/IAccumulatingTransform.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Processors/TTL/ITTLAlgorithm.h>
#include <Processors/TTL/TTLDeleteAlgorithm.h>

namespace DB
{

class Block;

class TTLTransform final : public IAccumulatingTransform
{
public:
    TTLTransform(
        const ContextPtr & context,
        SharedHeader header_,
        const MergeTreeData & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeData::MutableDataPartPtr & data_part_,
        const NamesAndTypesList & expired_columns_,
        time_t current_time,
        bool force_
    );

    String getName() const override { return "TTL"; }

    Status prepare() override;

    PreparedSets::Subqueries getSubqueries() { return std::move(subqueries_for_sets); }

    static SharedHeader addExpiredColumnsToBlock(const SharedHeader & header, const NamesAndTypesList & expired_columns_);

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

    /// Finalizes ttl infos and updates data part
    void finalize();

private:
    std::vector<TTLAlgorithmPtr> algorithms;
    const TTLDeleteAlgorithm * delete_algorithm = nullptr;
    bool all_data_dropped = false;

    PreparedSets::Subqueries subqueries_for_sets;

    /// ttl_infos and empty_columns are updating while reading
    const MergeTreeData::MutableDataPartPtr & data_part;

    NamesAndTypesList expired_columns;

    struct ExpiredColumnData
    {
        DataTypePtr type;
        ExpressionActionsPtr default_expression;
        String default_column_name;
    };
    std::unordered_map<String, ExpiredColumnData> expired_columns_data;

    /// A `MATERIALIZED` column whose expression reads a column that a column TTL resets to its default.
    /// Its stored value describes the pre-expiry data, so it is recomputed once the TTL algorithms that
    /// reset the columns it reads have run.
    struct DependentMaterializedColumn
    {
        String name;
        ExpressionActionsPtr expression;
        String result_column_name;
        Names required_columns;
    };

    /// The dependents to recompute at one point of the `algorithms` sequence.
    struct RecomputeStage
    {
        /// Run this stage right before `algorithms[before_algorithm]`.
        size_t before_algorithm;
        std::vector<DependentMaterializedColumn> columns;
    };

    /// Ordered by `before_algorithm`.
    std::vector<RecomputeStage> recompute_stages;

    /// The `MATERIALIZED` columns that resetting `reset_columns` makes stale, grouped into levels: a
    /// dependent may read another dependent (`m2 MATERIALIZED m1 + 1`), and a column of level `i` reads
    /// only columns of the lower levels, so recomputing level by level always reads an already
    /// recomputed value. `columns_to_leave_alone` are never recomputed, even when they are stale.
    static std::vector<std::vector<DependentMaterializedColumn>> analyzeDependentMaterializedColumns(
        const MergeTreeData & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const NameSet & reset_columns,
        const NameSet & columns_to_leave_alone);

    /// Recomputes `dependents` in the block the TTL algorithms have already worked on.
    static void recomputeDependentColumns(Block & block, const std::vector<DependentMaterializedColumn> & dependents);

    /// Runs the TTL algorithms, recomputing the dependents in between.
    void executeAlgorithms(Block & block);

    LoggerPtr log;
};

}
