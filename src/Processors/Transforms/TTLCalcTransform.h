#pragma once

#include <Interpreters/PreparedSets.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/TTL/ITTLAlgorithm.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>

namespace DB
{

class Block;

class TTLCalcTransform final : public IAccumulatingTransform
{
public:
    TTLCalcTransform(
        const ContextPtr & context,
        SharedHeader header_,
        const MergeTreeData & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeData::MutableDataPartPtr & data_part_,
        time_t current_time,
        bool force_,
        const NamesAndTypesList & expired_columns_ = {}
    );

    PreparedSets::Subqueries getSubqueries() { return std::move(subqueries_for_sets); }

    String getName() const override { return "TTL_CALC"; }
    Status prepare() override;

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

    /// Finalizes ttl infos and updates data part
    void finalize();

private:
    std::vector<TTLAlgorithmPtr> algorithms;
    /// Physically dropped as fully expired; rules reading them evaluate over defaults, as in TTLTransform.
    NamesAndTypesList expired_columns;
    struct ExpiredColumnData
    {
        DataTypePtr type;
        ExpressionActionsPtr default_expression;
        String default_column_name;
    };
    std::unordered_map<String, ExpiredColumnData> expired_columns_data;
    /// (column name, pre-merge info) for column rules whose inputs are absent from the stream.
    std::vector<std::pair<String, MergeTreeDataPartTTLInfo>> preserved_column_ttls;
    PreparedSets::Subqueries subqueries_for_sets;

    /// ttl_infos and empty_columns are updating while reading
    const MergeTreeData::MutableDataPartPtr & data_part;
    LoggerPtr log;
};

}
