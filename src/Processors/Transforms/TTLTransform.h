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
        bool apply_ttl_,
        bool force_,
        bool ttl_delete_applied_by_merge_ = false
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
    /// If false, only fills default values for `expired_columns`: no TTL algorithm runs and the part keeps its TTL infos.
    /// TODO: move the fill into its own step, build `TTLStep` only when the merge applies TTL, and drop this flag.
    const bool apply_ttl = true;
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
    std::unordered_map<String, ExpiredColumnData> expired_columns_data;
    LoggerPtr log;
};

}
