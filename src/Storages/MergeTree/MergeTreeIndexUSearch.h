#pragma once

#include "config.h"

#if USE_USEARCH

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpass-failed"
#  include <Storages/MergeTree/ApproximateNearestNeighborIndexesCommon.h>
#  include <Common/Logger.h>
#  include <usearch/index_dense.hpp>
#pragma clang diagnostic pop

namespace DB
{

using USearchIndex = unum::usearch::index_dense_gt</*key_at*/ uint32_t, /*compressed_slot_at*/ uint32_t>;

class USearchIndexWithSerialization : public USearchIndex
{
    using Base = USearchIndex;

public:
    USearchIndexWithSerialization(
        size_t dimensions,
        unum::usearch::metric_kind_t metric_kind,
        unum::usearch::scalar_kind_t scalar_kind);

    void serialize(WriteBuffer & ostr) const;
    void deserialize(ReadBuffer & istr);

    struct Statistics
    {
        size_t max_level;
        size_t connectivity;
        size_t size;
        size_t capacity;
        size_t memory_usage;
        /// advanced stats:
        size_t bytes_per_vector;
        size_t scalar_words;
        Base::stats_t statistics;
    };

    Statistics getStatistics() const;
};

using USearchIndexWithSerializationPtr = std::shared_ptr<USearchIndexWithSerialization>;


struct MergeTreeIndexGranuleUSearch final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleUSearch(
        const String & index_name_,
        const Block & index_sample_block_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_);

    MergeTreeIndexGranuleUSearch(
        const String & index_name_,
        const Block & index_sample_block_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        USearchIndexWithSerializationPtr index_);

    ~MergeTreeIndexGranuleUSearch() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !index || index->size() == 0; }

    const String index_name;
    const Block index_sample_block;
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    USearchIndexWithSerializationPtr index;

    LoggerPtr logger = getLogger("USearchIndex");

private:
    /// The version of the persistence format of USearch index. Increment whenever you change the format.
    /// Note: USearch prefixes the serialized data with its own version header. We can't rely on that because 1. the index in ClickHouse
    /// is (at least in theory) agnostic of specific vector search libraries, and 2. additional data (e.g. the number of dimensions)
    /// outside USearch exists which we should version separately.
    static constexpr UInt64 FILE_FORMAT_VERSION = 1;
};


struct MergeTreeIndexAggregatorUSearch final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorUSearch(
        const String & index_name_,
        const Block & index_sample_block,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_);

    ~MergeTreeIndexAggregatorUSearch() override = default;

    bool empty() const override { return !index || index->size() == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const String index_name;
    const Block index_sample_block;
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
    USearchIndexWithSerializationPtr index;
};


class MergeTreeIndexConditionUSearch final : public IMergeTreeIndexConditionApproximateNearestNeighbor
{
public:
    MergeTreeIndexConditionUSearch(
        const IndexDescription & index_description,
        const SelectQueryInfo & query,
        unum::usearch::metric_kind_t metric_kind_,
        ContextPtr context);

    ~MergeTreeIndexConditionUSearch() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override;
    std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr granule) const override;

private:
    const ApproximateNearestNeighborCondition ann_condition;
    const unum::usearch::metric_kind_t metric_kind;
};


class MergeTreeIndexUSearch : public IMergeTreeIndex
{
public:
    MergeTreeIndexUSearch(
        const IndexDescription & index_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_);

    ~MergeTreeIndexUSearch() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG *, ContextPtr) const override;
    bool isVectorSearch() const override { return true; }

private:
    const unum::usearch::metric_kind_t metric_kind;
    const unum::usearch::scalar_kind_t scalar_kind;
};

}


#endif
