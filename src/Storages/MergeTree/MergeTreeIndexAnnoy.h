#pragma once


#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndicesANNCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <annoylib.h>
#include <kissrandom.h>


namespace DB
{


namespace Annoy
{
    using AnnoyIndexThreadedBuildPolicy = ::Annoy::AnnoyIndexSingleThreadedBuildPolicy;
    template <typename Dist = ::Annoy::Euclidean>
    class AnnoyIndexSerialize : public ::Annoy::AnnoyIndex<Int32, Float32, Dist, ::Annoy::Kiss64Random, AnnoyIndexThreadedBuildPolicy>
    {
        using Base = ::Annoy::AnnoyIndex<Int32, Float32, Dist, ::Annoy::Kiss64Random, AnnoyIndexThreadedBuildPolicy>;
    public:
        AnnoyIndexSerialize() = delete;
        AnnoyIndexSerialize(const int dim) : Base::AnnoyIndex(dim) {}
        void serialize(WriteBuffer& ostr) const;
        void deserialize(ReadBuffer& istr);
        float getSpaceDim() const;
    };
}

struct MergeTreeIndexGranuleAnnoy final : public IMergeTreeIndexGranule
{
    using AnnoyIndex = Annoy::AnnoyIndexSerialize<>;
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
    using AnnoyIndex = Annoy::AnnoyIndexSerialize<>;
    using AnnoyIndexPtr = std::shared_ptr<AnnoyIndex>;

    MergeTreeIndexAggregatorAnnoy(const String & index_name_, const Block & index_sample_block);
    ~MergeTreeIndexAggregatorAnnoy() override = default;

    bool empty() const override;
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    AnnoyIndexPtr index_base;
};


class MergeTreeIndexConditionAnnoy final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionAnnoy(
        const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionAnnoy() override = default;

private:
    ANNCondition::ANNCondition condition;
};


class MergeTreeIndexAnnoy : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexAnnoy(const IndexDescription & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexAnnoy() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;
};


}
