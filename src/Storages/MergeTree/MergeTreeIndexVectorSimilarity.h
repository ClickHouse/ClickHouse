#pragma once

#include "config.h"

#if USE_USEARCH

#include <Storages/MergeTree/VectorSimilarityCondition.h>
#include <Common/Logger.h>
#include <usearch/index_dense.hpp>

namespace DB
{

struct UsearchHnswParams
{
    size_t connectivity = unum::usearch::default_connectivity();
    size_t expansion_add = unum::usearch::default_expansion_add();
};

using USearchIndex = unum::usearch::index_dense_t;

class USearchIndexWithSerialization : public USearchIndex
{
    using Base = USearchIndex;

public:
    USearchIndexWithSerialization(
        size_t dimensions,
        unum::usearch::metric_kind_t metric_kind,
        unum::usearch::scalar_kind_t scalar_kind,
        UsearchHnswParams usearch_hnsw_params);

    void serialize(WriteBuffer & ostr) const;
    void deserialize(ReadBuffer & istr);

    struct Statistics
    {
        size_t max_level;
        size_t connectivity;
        size_t size;                /// number of indexed vectors
        size_t capacity;            /// reserved number of indexed vectors
        size_t memory_usage;        /// byte size (not exact)
        size_t bytes_per_vector;
        size_t scalar_words;
        size_t nodes;
        size_t edges;
        size_t max_edges;

        std::vector<USearchIndex::stats_t> level_stats; /// for debugging, excluded from getStatistics()

        String toString() const;
    };

    Statistics getStatistics() const;
};

using USearchIndexWithSerializationPtr = std::shared_ptr<USearchIndexWithSerialization>;


struct MergeTreeIndexGranuleVectorSimilarity final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleVectorSimilarity(
        const String & index_name_,
        const Block & index_sample_block_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_);

    MergeTreeIndexGranuleVectorSimilarity(
        const String & index_name_,
        const Block & index_sample_block_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_,
        USearchIndexWithSerializationPtr index_);

    ~MergeTreeIndexGranuleVectorSimilarity() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !index || index->size() == 0; }

    const String index_name;
    const Block index_sample_block;
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    const UsearchHnswParams usearch_hnsw_params;
    USearchIndexWithSerializationPtr index;

    LoggerPtr logger = getLogger("VectorSimilarityIndex");

private:
    /// The version of the persistence format of USearch index. Increment whenever you change the format.
    /// Note: USearch prefixes the serialized data with its own version header. We can't rely on that because 1. the index in ClickHouse
    /// is (at least in theory) agnostic of specific vector search libraries, and 2. additional data (e.g. the number of dimensions)
    /// outside USearch exists which we should version separately.
    static constexpr UInt64 FILE_FORMAT_VERSION = 1;
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


class MergeTreeIndexConditionVectorSimilarity final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionVectorSimilarity(
        const IndexDescription & index_description,
        const SelectQueryInfo & query,
        unum::usearch::metric_kind_t metric_kind_,
        ContextPtr context);

    ~MergeTreeIndexConditionVectorSimilarity() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override;
    std::vector<UInt64> calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule) const override;

private:
    const VectorSimilarityCondition vector_similarity_condition;
    const unum::usearch::metric_kind_t metric_kind;
    const size_t expansion_search;
};


class MergeTreeIndexVectorSimilarity : public IMergeTreeIndex
{
public:
    MergeTreeIndexVectorSimilarity(
        const IndexDescription & index_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_);

    ~MergeTreeIndexVectorSimilarity() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG *, ContextPtr) const override;
    bool isVectorSimilarityIndex() const override { return true; }

private:
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    const UsearchHnswParams usearch_hnsw_params;
};

}


#endif
