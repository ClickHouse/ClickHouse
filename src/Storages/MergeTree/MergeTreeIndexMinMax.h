#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>


namespace DB
{

struct MergeTreeIndexGranuleMinMax final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleMinMax(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleMinMax(
        const String & index_name_,
        const Block & index_sample_block_,
        std::vector<Range> && hyperrectangle_);

    ~MergeTreeIndexGranuleMinMax() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return hyperrectangle.empty(); }

    String index_name;
    Block index_sample_block;
    std::vector<Range> hyperrectangle;
};


struct MergeTreeIndexAggregatorMinMax final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorMinMax(const String & index_name_, const Block & index_sample_block);
    ~MergeTreeIndexAggregatorMinMax() override = default;

    bool empty() const override { return hyperrectangle.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    std::vector<Range> hyperrectangle;
};


class MergeTreeIndexConditionMinMax final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionMinMax(
        const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionMinMax() override = default;
private:
    DataTypes index_data_types;
    KeyCondition condition;
};


class MergeTreeIndexMinMax : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexMinMax(const IndexDescription & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexMinMax() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override; /// NOLINT
};

}
