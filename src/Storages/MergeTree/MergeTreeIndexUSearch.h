#pragma once

#ifdef ENABLE_USEARCH

#include <Storages/MergeTree/ApproximateNearestNeighborIndexesCommon.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpass-failed"
#include <usearch/index_dense.hpp>
#pragma clang diagnostic pop

namespace DB
{

using USearchImplType = unum::usearch::index_dense_gt</* key_at */ uint32_t, /* compressed_slot_at */ uint32_t>;

template <unum::usearch::metric_kind_t Metric>
class USearchIndexWithSerialization : public USearchImplType
{
    using Base = USearchImplType;

public:
    USearchIndexWithSerialization(size_t dimensions, unum::usearch::scalar_kind_t scalar_kind);
    void serialize(WriteBuffer & ostr) const;
    void deserialize(ReadBuffer & istr);
    size_t getDimensions() const;
};

template <unum::usearch::metric_kind_t Metric>
using USearchIndexWithSerializationPtr = std::shared_ptr<USearchIndexWithSerialization<Metric>>;


template <unum::usearch::metric_kind_t Metric>
struct MergeTreeIndexGranuleUSearch final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleUSearch(const String & index_name_, const Block & index_sample_block_, unum::usearch::scalar_kind_t scalar_kind_);
    MergeTreeIndexGranuleUSearch(const String & index_name_, const Block & index_sample_block_, unum::usearch::scalar_kind_t scalar_kind_, USearchIndexWithSerializationPtr<Metric> index_);

    ~MergeTreeIndexGranuleUSearch() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !index.get(); }

    const String index_name;
    const Block index_sample_block;
    const unum::usearch::scalar_kind_t scalar_kind;
    USearchIndexWithSerializationPtr<Metric> index;
};


template <unum::usearch::metric_kind_t Metric>
struct MergeTreeIndexAggregatorUSearch final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorUSearch(const String & index_name_, const Block & index_sample_block, unum::usearch::scalar_kind_t scalar_kind_);
    ~MergeTreeIndexAggregatorUSearch() override = default;

    bool empty() const override { return !index || index->size() == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const String index_name;
    const Block index_sample_block;
    const unum::usearch::scalar_kind_t scalar_kind;
    USearchIndexWithSerializationPtr<Metric> index;
};


class MergeTreeIndexConditionUSearch final : public IMergeTreeIndexConditionApproximateNearestNeighbor
{
public:
    MergeTreeIndexConditionUSearch(
        const IndexDescription & index_description,
        const SelectQueryInfo & query,
        const String & distance_function,
        ContextPtr context);

    ~MergeTreeIndexConditionUSearch() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;
    std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const override;

private:
    template <unum::usearch::metric_kind_t Metric>
    std::vector<size_t> getUsefulRangesImpl(MergeTreeIndexGranulePtr idx_granule) const;

    const ApproximateNearestNeighborCondition ann_condition;
    const String distance_function;
};


class MergeTreeIndexUSearch : public IMergeTreeIndex
{
public:
    MergeTreeIndexUSearch(const IndexDescription & index_, const String & distance_function_, unum::usearch::scalar_kind_t scalar_kind_);

    ~MergeTreeIndexUSearch() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAGPtr &, ContextPtr) const override;
    bool isVectorSearch() const override { return true; }

private:
    const String distance_function;
    const unum::usearch::scalar_kind_t scalar_kind;
};

}


#endif

