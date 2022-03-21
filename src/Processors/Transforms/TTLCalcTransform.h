#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Core/Block.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Processors/TTL/ITTLAlgorithm.h>

#include <Common/DateLUT.h>

namespace DB
{

class TTLCalcTransform : public IAccumulatingTransform
{
public:
    TTLCalcTransform(
        const Block & header_,
        const MergeTreeData & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeData::MutableDataPartPtr & data_part_,
        time_t current_time,
        bool force_
    );

    String getName() const override { return "TTL_CALC"; }
    Status prepare() override;

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

    /// Finalizes ttl infos and updates data part
    void finalize();

private:
    std::vector<TTLAlgorithmPtr> algorithms;

    /// ttl_infos and empty_columns are updating while reading
    const MergeTreeData::MutableDataPartPtr & data_part;
    Poco::Logger * log;
};

}
