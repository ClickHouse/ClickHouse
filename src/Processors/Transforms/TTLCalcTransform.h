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
        /// Refresh only the rows-WHERE TTL infos and leave every other kind alone. Used by merges
        /// that combine rows while TTL merges are stopped, where the rows-WHERE info inherited from
        /// the source parts does not describe the merge output.
        bool only_rows_where_ttl_ = false
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
    PreparedSets::Subqueries subqueries_for_sets;

    /// ttl_infos and empty_columns are updating while reading
    const MergeTreeData::MutableDataPartPtr & data_part;
    const bool only_rows_where_ttl;
    LoggerPtr log;
};

}
