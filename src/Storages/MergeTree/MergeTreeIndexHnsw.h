#pragma once

#ifdef ENABLE_NMSLIB

#include <Storages/MergeTree/CommonANNIndexes.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <method/hnsw.h>
#include <knnqueue.h>

namespace DB
{

namespace ApproximateNearestNeighbor
{

template <typename Dist>
class HNSWIndex
{
public:
    HNSWIndex(const std::string & space_type_, const similarity::AnyParams & space_params = similarity::AnyParams());

    void createIndex(const similarity::AnyParams & params = similarity::AnyParams());

    void loadIndex(DB::ReadBuffer & istr);

    void saveIndex(DB::WriteBuffer & ostr);

    similarity::KNNQueue<Dist> * knnQuery(const similarity::Object & obj, size_t k);

    void replaceDataWith(similarity::ObjectVector && new_data);

    size_t dataSize() const;

    ~HNSWIndex();

private:
    /// remove data before reading index and in destructor
    void freeAndClearObjectVector();

    std::string space_type;
    std::unique_ptr<similarity::Space<Dist>> space;
    similarity::ObjectVector data;
    std::unique_ptr<similarity::Hnsw<Dist>> index;
};

}

struct MergeTreeIndexGranuleHnsw final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleHnsw(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleHnsw(
        const String & index_name_, const Block & index_sample_block_, std::unique_ptr<ApproximateNearestNeighbor::HNSWIndex<float>> index_impl_);

    ~MergeTreeIndexGranuleHnsw() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return false; }

    String index_name;
    Block index_sample_block;
    std::unique_ptr<ApproximateNearestNeighbor::HNSWIndex<float>> index_impl;
};

struct MergeTreeIndexAggregatorHnsw final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorHnsw(
        const String & index_name_, const Block & index_sample_block, const similarity::AnyParams & index_params_);
    ~MergeTreeIndexAggregatorHnsw() override = default;

    bool empty() const override { return data.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    similarity::AnyParams index_params;
    similarity::ObjectVector data;
};

class MergeTreeIndexConditionHnsw final : public ApproximateNearestNeighbour::IMergeTreeIndexConditionAnn
{
public:
    MergeTreeIndexConditionHnsw(const IndexDescription & index, const SelectQueryInfo & query, ContextPtr context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionHnsw() override = default;

private:
    ApproximateNearestNeighbour::ANNCondition condition;
};

class MergeTreeIndexHnsw : public IMergeTreeIndex
{
public:
    MergeTreeIndexHnsw(const IndexDescription & index_, const similarity::AnyParams & index_params_)
        : IMergeTreeIndex(index_), index_params(index_params_) {}

    ~MergeTreeIndexHnsw() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return false; }

private:
    similarity::AnyParams index_params;
};

}

#endif
