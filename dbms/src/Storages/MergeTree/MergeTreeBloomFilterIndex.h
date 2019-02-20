#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <memory>


namespace DB
{

class MergeTreeBloomFilterIndex;


struct MergeTreeBloomFilterIndexGranule : public IMergeTreeIndexGranule
{
    explicit MergeTreeBloomFilterIndexGranule(const MergeTreeBloomFilterIndex & index);

    ~MergeTreeBloomFilterIndexGranule() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;

    void deserializeBinary(ReadBuffer & istr) override;

    bool empty() const override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const MergeTreeBloomFilterIndex & index;
    StringBloomFilter bloom_filter;
};


class BloomFilterCondition : public IIndexCondition
{
public:
    BloomFilterCondition(
            const SelectQueryInfo &,
            const Context &,
            const MergeTreeBloomFilterIndex &) {};

    ~BloomFilterCondition() override = default;

    bool alwaysUnknownOrTrue() const override { return true; };

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr) const override { return true; };
private:
    // const MergeTreeBloomFilterIndex & index;
};

using TokenExtractor = std::function<
        bool(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len)>;

class MergeTreeBloomFilterIndex : public IMergeTreeIndex
{
public:
    MergeTreeBloomFilterIndex(
            String name_,
            ExpressionActionsPtr expr_,
            const Names & columns_,
            const DataTypes & data_types_,
            const Block & header_,
            size_t granularity_,
            size_t bloom_filter_size_,
            size_t bloom_filter_hashes_,
            size_t seed_,
            TokenExtractor tokenExtractorFunc_)
            : IMergeTreeIndex(name_, expr_, columns_, data_types_, header_, granularity_)
            , bloom_filter_size(bloom_filter_size_)
            , bloom_filter_hashes(bloom_filter_hashes_)
            , seed(seed_)
            , tokenExtractorFunc(tokenExtractorFunc_) {}

    ~MergeTreeBloomFilterIndex() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    IndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, const Context & context) const override;

    /// Bloom filter size in bytes.
    size_t bloom_filter_size;
    /// Number of bloom filter hash functions.
    size_t bloom_filter_hashes;
    /// Bloom filter seed.
    size_t seed;
    /// Fucntion for selecting next token
    TokenExtractor tokenExtractorFunc;
};

}
