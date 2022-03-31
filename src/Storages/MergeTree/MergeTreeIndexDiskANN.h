#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>
#include <random>
#include <string_view>

#include "index.h"

namespace DB
{

struct MergeTreeIndexGranuleDiskANN final : public IMergeTreeIndexGranule
{
    using DiskANNIndex = diskann::Index<Float32>;
    using DiskANNIndexPtr = std::shared_ptr<DiskANNIndex>;

    MergeTreeIndexGranuleDiskANN(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleDiskANN(const String & index_name_, const Block & index_sample_block_, DiskANNIndexPtr base_index_);

    ~MergeTreeIndexGranuleDiskANN() override = default;

    void serializeBinary(WriteBuffer & /*ostr*/) const override {}
    void deserializeBinary(ReadBuffer & /*istr*/, MergeTreeIndexVersion /*version*/) override {}
    bool empty() const override { return false; }

    String index_name;
    Block index_sample_block;
    DiskANNIndexPtr base_index;
};


struct MergeTreeIndexAggregatorDiskANN final : IMergeTreeIndexAggregator
{
    // TODO: Working only with Float32 type
    using Value = Float32;

    MergeTreeIndexAggregatorDiskANN(const String & index_name_, const Block & index_sample_block);
    ~MergeTreeIndexAggregatorDiskANN() override = default;

    bool empty() const override { /*return accumulated_data.empty();*/ return accumulated_data.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    void flattenAccumulatedData(std::vector<std::vector<Value>> data);
    void dumpDataToFile(std::string_view filename);

private:
    String index_name;
    Block index_sample_block;

    std::optional<uint32_t> dimensions;
    std::vector<Value> accumulated_data;
};


class MergeTreeIndexConditionDiskANN final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionDiskANN() = default;

    bool alwaysUnknownOrTrue() const override { return false; }

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionDiskANN() override = default;
private:
};


class MergeTreeIndexDiskANN : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexDiskANN(const IndexDescription & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexDiskANN() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;
};

}
