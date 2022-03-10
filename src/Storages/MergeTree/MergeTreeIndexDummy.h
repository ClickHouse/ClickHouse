#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>
#include "Storages/MergeTree/MergeTreeIndexGranuleBloomFilter.h"

#include <memory>

namespace DB{
    struct MergeTreeIndexGranuleDummy final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleDummy(const String & index_name_, const Block & index_sample_block_);
 
    ~MergeTreeIndexGranuleDummy() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return false;}

    String index_name;
    Block index_sample_block;
};

struct MergeTreeIndexAggregatorDummy final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorDummy(const String & index_name_, const Block & index_sample_block);
    ~MergeTreeIndexAggregatorDummy() override = default;

    bool empty() const override { return true; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
};

class MergeTreeIndexConditionDummy final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionDummy(const IndexDescription & index,  const SelectQueryInfo & query,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionDummy() override = default;
private:
    DataTypes index_data_types;
};

class MergeTreeIndexDummy : public IMergeTreeIndex
{
public:
    MergeTreeIndexDummy(const IndexDescription & index_) : IMergeTreeIndex(index_){}

    ~MergeTreeIndexDummy() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;
};


}
