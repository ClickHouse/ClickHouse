#pragma once

#ifdef ENABLE_USEARCH

#include <Storages/MergeTree/VectorSimilarityCommon.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpass-failed"
#include <usearch/index_dense.hpp>
#pragma clang diagnostic pop

namespace DB
{

struct UsearchHnswParams
{
    size_t m = unum::usearch::default_connectivity();
    size_t ef_construction = unum::usearch::default_expansion_add();
    size_t ef_search = unum::usearch::default_expansion_search();
};

using USearchIndexImpl = unum::usearch::index_dense_gt</*key_at*/ uint32_t, /*compressed_slot_at*/ uint32_t>;

class USearchIndexWithSerialization : public USearchIndexImpl
{
    using Base = USearchIndexImpl;

public:
    USearchIndexWithSerialization(
            size_t dimensions,
            unum::usearch::metric_kind_t metric_kind,
            unum::usearch::scalar_kind_t scalar_kind,
            UsearchHnswParams usearch_hnsw_params);
    void serialize(WriteBuffer & ostr) const;
    void deserialize(ReadBuffer & istr);
};

using USearchIndexWithSerializationPtr = std::shared_ptr<USearchIndexWithSerialization>;


struct MergeTreeIndexGranuleVectorSimilarity final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleVectorSimilarity(
        const String & index_name_,
        const Block & index_sample_block_,
        unum::usearch::metric_kind_t metric_kind,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_);
    MergeTreeIndexGranuleVectorSimilarity(
        const String & index_name_,
        const Block & index_sample_block_,
        unum::usearch::metric_kind_t metric_kind,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_,
        USearchIndexWithSerializationPtr index_);
    ~MergeTreeIndexGranuleVectorSimilarity() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !index.get(); }

    const String index_name;
    const Block index_sample_block;
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    const UsearchHnswParams usearch_hnsw_params;
    USearchIndexWithSerializationPtr index;
};


struct MergeTreeIndexAggregatorVectorSimilarity final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorVectorSimilarity(
        const String & index_name_,
        const Block & index_sample_block,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_);
    ~MergeTreeIndexAggregatorVectorSimilarity() override = default;

    bool empty() const override { return !index || index->size() == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const String index_name;
    const Block index_sample_block;
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    const UsearchHnswParams usearch_hnsw_params;
    USearchIndexWithSerializationPtr index;
};


class MergeTreeIndexConditionVectorSimilarity final : public IMergeTreeIndexConditionVectorSimilarity
{
public:
    MergeTreeIndexConditionVectorSimilarity(
        const IndexDescription & index_description,
        const SelectQueryInfo & query,
        unum::usearch::metric_kind_t metric_kind_,
        ContextPtr context);
    ~MergeTreeIndexConditionVectorSimilarity() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;
    std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const override;

private:
    const VectorSimilarityCondition condition;
    const unum::usearch::metric_kind_t metric_kind;
};


class MergeTreeIndexVectorSimilarity : public IMergeTreeIndex
{
public:
    MergeTreeIndexVectorSimilarity(
            const IndexDescription & index_,
            unum::usearch::metric_kind_t metric_kind_,
            unum::usearch::scalar_kind_t scalar_kind_,
            UsearchHnswParams usearch_hnsw_params);
    ~MergeTreeIndexVectorSimilarity() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAGPtr &, ContextPtr) const override;

    bool isVectorSearch() const override { return true; }

private:
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    const UsearchHnswParams usearch_hnsw_params;
};

}


#endif
