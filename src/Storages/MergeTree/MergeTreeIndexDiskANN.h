#pragma once

#include <memory>
#include <random>
#include <string_view>

#include <IO/WriteBuffer.h>

#include <Storages/MergeTree/CommonANNIndexes.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <contrib/diskann/include/index.h>

namespace DB
{

using DiskANNIndex = diskann::Index<Float32>;
using DiskANNIndexPtr = std::shared_ptr<DiskANNIndex>;

// !TODO: Working only with Float32 type
using DiskANNValue = Float32;

enum DiskANNArguments
{
    NUM_THREADS = 0,
    ALPHA = 1,
    GRAPH_DEGREE = 2,
    SEARCH_LIST_SIZE = 3,
    PRUNING_SET_SIZE = 4,
};

struct MergeTreeIndexGranuleDiskANN final : public IMergeTreeIndexGranule
{
    struct DiskANNSearchResult
    {
        std::vector<float> distances;
        std::vector<uint64_t> indicies;
    };

    MergeTreeIndexGranuleDiskANN(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleDiskANN(
        const String & index_name_, const Block & index_sample_block_,
        DiskANNIndexPtr base_index_, uint32_t dimensions, std::vector<DiskANNValue> datapoints
    );

    ~MergeTreeIndexGranuleDiskANN() override = default;

    void serializeBinary(WriteBuffer & out) const override;
    uint64_t calculateIndexSize() const;

    void deserializeBinary(ReadBuffer & in, MergeTreeIndexVersion version) override;
    bool empty() const override { return false; }

    DiskANNSearchResult searchVector(std::vector<float> target_vector, size_t neighbours_to_search) const;

    std::optional<uint32_t> dimensions;
    std::vector<DiskANNValue> datapoints;

    String index_name;
    Block index_sample_block;
    DiskANNIndexPtr base_index;
};


struct MergeTreeIndexAggregatorDiskANN final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorDiskANN(const String & index_name_, const Block & index_sample_block_,
                                    unsigned num_threads_, float alpha_, unsigned graph_degree_,
                                    unsigned search_list_size_, unsigned pruning_set_size_);
    ~MergeTreeIndexAggregatorDiskANN() override = default;

    bool empty() const override { return accumulated_data.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    void flattenAccumulatedData(std::vector<std::vector<DiskANNValue>> data);

    String index_name;
    Block index_sample_block;

    unsigned num_threads;
    float alpha;
    unsigned graph_degree;
    unsigned search_list_size;
    unsigned pruning_set_size;

    std::optional<uint32_t> dimensions;
    std::vector<DiskANNValue> accumulated_data;
};


class MergeTreeIndexConditionDiskANN final : public IMergeTreeIndexConditionAnn
{
public:
    MergeTreeIndexConditionDiskANN(
        const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context
    );

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;
    std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionDiskANN() override = default;
private:
    ANNCondition::ANNCondition common_condition;
};


class MergeTreeIndexDiskANN : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexDiskANN(const IndexDescription & index_, unsigned num_threads_, float alpha_,
                                   unsigned graph_degree_, unsigned search_list_size_, unsigned pruning_set_size_)
        : IMergeTreeIndex(index_)
        , num_threads(num_threads_)
        , alpha(alpha_)
        , graph_degree(graph_degree_)
        , search_list_size(search_list_size_)
        , pruning_set_size(pruning_set_size_)
    {}

    ~MergeTreeIndexDiskANN() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;

private:
    unsigned num_threads;
    float alpha;
    unsigned graph_degree;
    unsigned search_list_size;
    unsigned pruning_set_size;
};

}
