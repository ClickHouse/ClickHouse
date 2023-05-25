#pragma once

#ifdef ENABLE_ANNOY

#include <Storages/MergeTree/CommonANNIndexes.h>

#include <annoylib.h>
#include <kissrandom.h>

namespace DB
{

template <typename Distance>
class AnnoyIndexWithSerialization : public ::Annoy::AnnoyIndex<UInt64, Float32, Distance, ::Annoy::Kiss64Random, ::Annoy::AnnoyIndexMultiThreadedBuildPolicy>
{
    using Base = ::Annoy::AnnoyIndex<UInt64, Float32, Distance, ::Annoy::Kiss64Random, ::Annoy::AnnoyIndexMultiThreadedBuildPolicy>;

public:
    explicit AnnoyIndexWithSerialization(uint64_t dim);
    void serialize(WriteBuffer& ostr) const;
    void deserialize(ReadBuffer& istr);
    uint64_t getNumOfDimensions() const;
};

template <typename Distance>
using AnnoyIndexWithSerializationPtr = std::shared_ptr<AnnoyIndexWithSerialization<Distance>>;

template <typename Distance>
struct MergeTreeIndexGranuleAnnoy final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleAnnoy(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleAnnoy(const String & index_name_, const Block & index_sample_block_, AnnoyIndexWithSerializationPtr<Distance> index_);

    ~MergeTreeIndexGranuleAnnoy() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !index.get(); }

    String index_name;
    Block index_sample_block;
    AnnoyIndexWithSerializationPtr<Distance> index;
};

template <typename Distance>
struct MergeTreeIndexAggregatorAnnoy final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorAnnoy(const String & index_name_, const Block & index_sample_block, uint64_t trees);
    ~MergeTreeIndexAggregatorAnnoy() override = default;

    bool empty() const override { return !index || index->get_n_items() == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    const uint64_t trees;
    AnnoyIndexWithSerializationPtr<Distance> index;
};


class MergeTreeIndexConditionAnnoy final : public ApproximateNearestNeighbour::IMergeTreeIndexConditionAnn
{
public:
    MergeTreeIndexConditionAnnoy(
        const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context,
        const String& distance_name);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionAnnoy() override = default;

private:
    template <typename Distance>
    std::vector<size_t> getUsefulRangesImpl(MergeTreeIndexGranulePtr idx_granule) const;

    ApproximateNearestNeighbour::ANNCondition condition;
    const String distance_name;
};


class MergeTreeIndexAnnoy : public IMergeTreeIndex
{
public:

    MergeTreeIndexAnnoy(const IndexDescription & index_, uint64_t trees_, const String& distance_name_);

    ~MergeTreeIndexAnnoy() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return false; }

private:
    const uint64_t trees;
    const String distance_name;
};


}

#endif
