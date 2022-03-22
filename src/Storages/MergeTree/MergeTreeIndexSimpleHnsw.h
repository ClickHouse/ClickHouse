#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>
#include "Storages/MergeTree/MergeTreeIndexMinMax.h"

#include <memory>
#include "object.h"
namespace DB
{

struct MergeTreeIndexGranuleSimpleHnsw final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleSimpleHnsw(const String & index_name_, const Block & index_sample_block_);
    ~MergeTreeIndexGranuleSimpleHnsw() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return true; }

    String index_name;
    Block index_sample_block;
    similarity::ObjectVector batch_data;
};

}
