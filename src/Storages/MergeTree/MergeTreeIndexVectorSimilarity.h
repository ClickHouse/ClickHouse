#pragma once

#ifdef ENABLE_USEARCH

#include <Storages/MergeTree/VectorSimilarityCondition.h>

#if defined(__linux__) && (defined(__x86_64__) || defined (__aarch64__))
#  define USEARCH_USE_SIMSIMD 1 /// probably works on other platforms too but let's not risk
/// #  define SIMSIMD_DYNAMIC_DISPATCH 1
/// TODO ^^ we should also enable dynamic dispatch to make the selected SIMD instructions independent of the build machine (which is limited to
/// SSE4.2) - I discovered in `perf top` that currently always the serial versions are chosen. This will require: 1. additionally build the
/// source file of SimSIMD which does the runtime detection 2. find out why USearch hard-codes SIMSIMD_DYNAMIC_DISPATCH = 0 in
/// index_plugins.hpp. That seems silly.
#  define USEARCH_USE_FP16LIB 0 /// native FP16 type (needs x86/ARM-only _Float16/__fp16 types)
#else
#  define USEARCH_USE_SIMSIMD 0
#  define USEARCH_USE_FP16LIB 1 /// software-emulated FP16 type
#endif

#define USEARCH_USE_OPENMP  0 /// ClickHouse uses its own thread pool

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

private:
    /// The version of the persistence format of vector similarity index. Increment whenever you change the format.
    /// Note: Usearch prefixes the serialized data with its own version header. We can't rely on that because 1. vector similarity indexes
    /// are (at least in theory) agnostic of specific vector search libraries, and 2. additional data (e.g. the number of dimensions)
    /// outside usearch exists which we should keep it separately versioned.
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

    bool isVectorSimilarityIndex() const override { return true; }

private:
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    const UsearchHnswParams usearch_hnsw_params;
};

}


#endif
