#pragma once

#include <Interpreters/PreparedSets.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
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
        bool force_,
        bool ttl_delete_applied_by_merge_ = false,
        const NameSet & columns_with_reader_defaults_ = {},
        const NameSet & expired_arrays_ = {}
    );

    String getName() const override { return "TTL"; }

    Status prepare() override;

    PreparedSets::Subqueries getSubqueries() { return std::move(subqueries_for_sets); }

    static SharedHeader addExpiredColumnsToBlock(const SharedHeader & header, const NamesAndTypesList & expired_columns_);

    /// What a transform built with the same arguments evaluates.
    struct EvaluatedExpressions
    {
        /// The columns whose `DEFAULT` or `MATERIALIZED` expression it evaluates: the expired ones, the ones whose
        /// column TTL is due, which `TTLColumnAlgorithm` resets to their default, and the dependents that it
        /// recomputes. A stale dependent that is expired, read by a key or non-deterministic is not recomputed.
        NameSet columns_with_evaluated_defaults;

        /// The columns that its `DELETE` rules and column TTLs read. Such a rule is evaluated only once it is due,
        /// unlike a `GROUP BY`, move or `RECOMPRESS` rule, which is evaluated by every merge.
        NameSet columns_read_by_due_ttls;
    };

    static EvaluatedExpressions getEvaluatedExpressions(
        const MergeTreeData & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeDataPartTTLInfos & ttl_infos,
        const NamesAndTypesList & expired_columns_,
        time_t current_time,
        bool force);

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

    /// Finalizes ttl infos and updates data part
    void finalize();

private:
    VectorWithMemoryTracking<TTLAlgorithmPtr> algorithms;
    size_t first_rewrite_algorithm = 0;
    const TTLDeleteAlgorithm * delete_algorithm = nullptr;
    bool all_data_dropped = false;
    /// The merging algorithm already dropped the expired rows, so `delete_algorithm` counts none.
    const bool ttl_delete_applied_by_merge = false;

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
    UnorderedMapWithMemoryTracking<String, ExpiredColumnData> expired_columns_data;
    UnorderedMapWithMemoryTracking<String, String> shared_offsets_sources;
    /// The siblings that write the offsets of an expired array, whether or not a reader filled that array.
    NameSet shared_offsets_writers;
    /// Writer-eligible sources with nonempty offsets in at least one post-TTL output block.
    NameSet shared_offsets_columns_to_keep;

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
        VectorWithMemoryTracking<DependentMaterializedColumn> columns;
    };

    /// Ordered by `before_algorithm`.
    VectorWithMemoryTracking<RecomputeStage> recompute_stages;

    /// The columns this transform may reset to their default: the ones with a column TTL and the expired ones.
    static NameSet getResetColumns(const StorageMetadataPtr & metadata_snapshot, const NamesAndTypesList & expired_columns_);

    /// The `MATERIALIZED` columns that resetting `reset_columns` makes stale, grouped into levels: a
    /// dependent may read another dependent (`m2 MATERIALIZED m1 + 1`), and a column of level `i` reads
    /// only columns of the lower levels, so recomputing level by level always reads an already
    /// recomputed value. `columns_to_leave_alone` are never recomputed, even when they are stale.
    static VectorWithMemoryTracking<VectorWithMemoryTracking<DependentMaterializedColumn>> analyzeDependentMaterializedColumns(
        const MergeTreeData & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const NameSet & reset_columns,
        const NameSet & columns_to_leave_alone,
        const UnorderedMapWithMemoryTracking<String, String> & shared_offsets_dependencies);

    /// Recomputes `dependents` in the block the TTL algorithms have already worked on.
    void recomputeDependentColumns(Block & block, const VectorWithMemoryTracking<DependentMaterializedColumn> & dependents);

    /// Refreshes omitted arrays from their output offsets sources, not before row TTL evaluation.
    void refreshSharedOffsetsColumns(Block & block, const String & source_column = {});

    /// Runs the TTL algorithms, recomputing the dependents in between.
    void executeAlgorithms(Block & block);

    LoggerPtr log;
};

}
