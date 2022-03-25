#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>
#include <random>

#include "index.h"

namespace DB
{

struct MergeTreeIndexGranuleSimple final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleSimple();

    ~MergeTreeIndexGranuleSimple() override = default;

    void serializeBinary(WriteBuffer & /*ostr*/) const override {}
    void deserializeBinary(ReadBuffer & /*istr*/, MergeTreeIndexVersion /*version*/) override {}
    bool empty() const override { return false; }

    bool is_useful_random;
};


struct MergeTreeIndexAggregatorSimple final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorSimple() = default;
    ~MergeTreeIndexAggregatorSimple() override = default;

    bool empty() const override { return true; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;
};


class MergeTreeIndexConditionSimple final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionSimple() = default;

    bool alwaysUnknownOrTrue() const override { return false; }

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionSimple() override = default;
private:
};


class MergeTreeIndexSimple : public IMergeTreeIndex
{
public:
    MergeTreeIndexSimple(const IndexDescription & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexSimple() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;
};

}
