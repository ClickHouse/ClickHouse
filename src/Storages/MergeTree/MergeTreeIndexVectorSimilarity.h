#pragma once

#include "config.h"

#if USE_USEARCH

#if USE_PDX
#include <pdx/index.hpp>
#endif

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/Logger.h>
#include <vector>

/// Include immintrin. Otherwise `simsimd` fails to build: `unknown type name '__bfloat16'`
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif
#include <usearch/index_dense.hpp>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

enum class VectorSimilarityMethod : UInt8
{
    HNSW = 0,
    PDX = 1,
};

/// Defaults for HNSW parameters. Instead of using the default parameters provided by USearch (default_connectivity(),
/// default_expansion_add(), default_expansion_search()), we experimentally came up with our own default parameters. They provide better
/// trade-offs with regards to index construction time, search precision and queries-per-second (speed).
static constexpr size_t default_connectivity = 32;
static constexpr size_t default_expansion_add = 128;
static constexpr size_t default_expansion_search = 256;

/// Parameters for HNSW index construction.
struct UsearchHnswParams
{
    size_t connectivity = default_connectivity;
    size_t expansion_add = default_expansion_add;
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

    size_t memoryUsageBytes() const;
};

using USearchIndexWithSerializationPtr = std::shared_ptr<USearchIndexWithSerialization>;


struct MergeTreeIndexGranuleVectorSimilarity final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleVectorSimilarity(
        const String & index_name_,
        VectorSimilarityMethod method_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_);

    MergeTreeIndexGranuleVectorSimilarity(
        const String & index_name_,
        VectorSimilarityMethod method_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_,
        USearchIndexWithSerializationPtr hnsw_index_
#if USE_PDX
        ,
        std::shared_ptr<PDX::IPDXIndex> pdx_index_
#endif
        );

    ~MergeTreeIndexGranuleVectorSimilarity() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override;

    size_t memoryUsageBytes() const override;

    const String index_name;
    const VectorSimilarityMethod method;
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    const UsearchHnswParams usearch_hnsw_params;
    USearchIndexWithSerializationPtr hnsw_index;
#if USE_PDX
    std::shared_ptr<PDX::IPDXIndex> pdx_index;
#endif

    LoggerPtr logger = getLogger("VectorSimilarityIndex");

private:
    static constexpr UInt64 FILE_FORMAT_VERSION = 2;
};


struct MergeTreeIndexAggregatorVectorSimilarity final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorVectorSimilarity(
        const String & index_name_,
        const Block & index_sample_block,
        VectorSimilarityMethod method_,
        UInt64 dimensions_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_);

    ~MergeTreeIndexAggregatorVectorSimilarity() override = default;

    bool empty() const override;
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const String index_name;
    const Block index_sample_block;
    const VectorSimilarityMethod method;
    const UInt64 dimensions;
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    const UsearchHnswParams usearch_hnsw_params;
    USearchIndexWithSerializationPtr hnsw_index;
#if USE_PDX
    std::vector<float> pdx_embeddings;
    size_t pdx_rows = 0;
    std::shared_ptr<PDX::IPDXIndex> pdx_index;
#endif
};


class MergeTreeIndexConditionVectorSimilarity final : public IMergeTreeIndexCondition
{
public:
    explicit MergeTreeIndexConditionVectorSimilarity(
        const std::optional<VectorSearchParameters> & parameters_,
        const String & index_column_,
        VectorSimilarityMethod method_,
        unum::usearch::metric_kind_t metric_kind_,
        ContextPtr context);

    ~MergeTreeIndexConditionVectorSimilarity() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;
    NearestNeighbours calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule) const override;
    std::string getDescription() const override { return ""; }

private:
    std::optional<VectorSearchParameters> parameters;
    const String index_column;
    const VectorSimilarityMethod method;
    const unum::usearch::metric_kind_t metric_kind;
    const size_t expansion_search;
    const float index_fetch_multiplier;
    const size_t max_limit;
    const bool is_rescoring;
};


class MergeTreeIndexVectorSimilarity : public IMergeTreeIndex
{
public:
    MergeTreeIndexVectorSimilarity(
        const IndexDescription & index_,
        VectorSimilarityMethod method_,
        UInt64 dimensions_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_);

    ~MergeTreeIndexVectorSimilarity() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const override;
    bool isVectorSimilarityIndex() const override { return true; }

private:
    const VectorSimilarityMethod method;
    const UInt64 dimensions;
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    const UsearchHnswParams usearch_hnsw_params;
};

}

#endif
