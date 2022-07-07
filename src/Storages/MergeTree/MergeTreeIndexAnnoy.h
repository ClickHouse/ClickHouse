#pragma once

#ifdef ENABLE_ANNOY

#include <Storages/MergeTree/CommonANNIndexes.h>

#include <annoylib.h>
#include <kissrandom.h>

namespace DB
{

// auxiliary namespace for working with spotify-annoy library
// mainly for serialization and deserialization of the index
namespace ApproximateNearestNeighbour
{
    using AnnoyIndexThreadedBuildPolicy = ::Annoy::AnnoyIndexSingleThreadedBuildPolicy;
    // TODO: Support different metrics. List of available metrics can be taken from here:
    // https://github.com/spotify/annoy/blob/master/src/annoymodule.cc#L151-L171
    template <typename Dist = ::Annoy::Euclidean>
    class AnnoyIndexSerialize : public ::Annoy::AnnoyIndex<Int32, Float32, Dist, ::Annoy::Kiss64Random, AnnoyIndexThreadedBuildPolicy>
    {
        using Base = ::Annoy::AnnoyIndex<Int32, Float32, Dist, ::Annoy::Kiss64Random, AnnoyIndexThreadedBuildPolicy>;
    public:
        AnnoyIndexSerialize() = delete;
        explicit AnnoyIndexSerialize(const int dim) : Base::AnnoyIndex(dim) {}
        void serialize(WriteBuffer& ostr) const;
        void deserialize(ReadBuffer& istr);
        float getNumOfDimensions() const;
    };
}

struct MergeTreeIndexGranuleAnnoy final : public IMergeTreeIndexGranule
{
    using AnnoyIndex = ANN::AnnoyIndexSerialize<>;
    using AnnoyIndexPtr = std::shared_ptr<AnnoyIndex>;

    MergeTreeIndexGranuleAnnoy(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleAnnoy(
        const String & index_name_,
        const Block & index_sample_block_,
        AnnoyIndexPtr index_base_);

    ~MergeTreeIndexGranuleAnnoy() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override;

    String index_name;
    Block index_sample_block;
    AnnoyIndexPtr index_base;
};


struct MergeTreeIndexAggregatorAnnoy final : IMergeTreeIndexAggregator
{
    using AnnoyIndex = ANN::AnnoyIndexSerialize<>;
    using AnnoyIndexPtr = std::shared_ptr<AnnoyIndex>;

    MergeTreeIndexAggregatorAnnoy(const String & index_name_, const Block & index_sample_block, int index_param);
    ~MergeTreeIndexAggregatorAnnoy() override = default;

    bool empty() const override;
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    int index_param;
    AnnoyIndexPtr index_base;
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
    MergeTreeIndexAnnoy(const IndexDescription & index_, int index_param_)
        : IMergeTreeIndex(index_)
        , index_param(index_param_)
    {}

    ~MergeTreeIndexAnnoy() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DataPartStoragePtr & data_part_storage, const std::string & path_prefix) const override;

private:
    const int index_param;
};


}

#endif // ENABLE_ANNOY
