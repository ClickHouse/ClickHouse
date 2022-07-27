#pragma once

#ifdef ENABLE_ANNOY

#include <Storages/MergeTree/CommonANNIndexes.h>

#define ANNOYLIB_MULTITHREADED_BUILD
#include <annoylib.h>
#include <kissrandom.h>

namespace DB
{

namespace ANN = ApproximateNearestNeighbour;

// auxiliary namespace for working with spotify-annoy library
// mainly for serialization and deserialization of the index
namespace ApproximateNearestNeighbour
{
    using AnnoyIndexThreadedBuildPolicy = ::Annoy::AnnoyIndexMultiThreadedBuildPolicy;
    // TODO: Support different metrics. List of available metrics can be taken from here:
    // https://github.com/spotify/annoy/blob/master/src/annoymodule.cc#L151-L171
    template <typename Distance = ::Annoy::Euclidean>
    class AnnoyIndex : public ::Annoy::AnnoyIndex<UInt64, Float32, Distance, ::Annoy::Kiss64Random, AnnoyIndexThreadedBuildPolicy>
    {
        using Base = ::Annoy::AnnoyIndex<UInt64, Float32, Distance, ::Annoy::Kiss64Random, AnnoyIndexThreadedBuildPolicy>;
    public:
        explicit AnnoyIndex(const uint64_t dim) : Base::AnnoyIndex(dim) {}
        void serialize(WriteBuffer& ostr) const;
        void deserialize(ReadBuffer& istr);
        uint64_t getNumOfDimensions() const;
    };
}

struct MergeTreeIndexGranuleAnnoy final : public IMergeTreeIndexGranule
{
    using AnnoyIndex = ANN::AnnoyIndex<>;
    using AnnoyIndexPtr = std::shared_ptr<AnnoyIndex>;

    MergeTreeIndexGranuleAnnoy(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleAnnoy(
        const String & index_name_,
        const Block & index_sample_block_,
        AnnoyIndexPtr index_base_);

    ~MergeTreeIndexGranuleAnnoy() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !index.get(); }

    String index_name;
    Block index_sample_block;
    AnnoyIndexPtr index;
};


struct MergeTreeIndexAggregatorAnnoy final : IMergeTreeIndexAggregator
{
    using AnnoyIndex = ANN::AnnoyIndex<>;
    using AnnoyIndexPtr = std::shared_ptr<AnnoyIndex>;

    MergeTreeIndexAggregatorAnnoy(const String & index_name_, const Block & index_sample_block, uint64_t number_of_trees);
    ~MergeTreeIndexAggregatorAnnoy() override = default;

    bool empty() const override { return !index || index->get_n_items() == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    const uint64_t number_of_trees;
    AnnoyIndexPtr index;
};


class MergeTreeIndexConditionAnnoy final : public ANN::IMergeTreeIndexConditionAnn
{
public:
    MergeTreeIndexConditionAnnoy(
        const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionAnnoy() override = default;

private:
    ANN::ANNCondition condition;
};


class MergeTreeIndexAnnoy : public IMergeTreeIndex
{
public:
    MergeTreeIndexAnnoy(const IndexDescription & index_, uint64_t number_of_trees_)
        : IMergeTreeIndex(index_)
        , number_of_trees(number_of_trees_)
    {}

    ~MergeTreeIndexAnnoy() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return false; }

private:
    const uint64_t number_of_trees;
};


}

#endif // ENABLE_ANNOY
