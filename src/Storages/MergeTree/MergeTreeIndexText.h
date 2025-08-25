#pragma once
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Columns/IColumn.h>
#include <Formats/MarkInCompressedFile.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilterText.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

struct MergeTreeIndexTextSparseIndex
{
    ColumnPtr terms;
    PaddedPODArray<MarkInCompressedFile> marks_to_blocks;

    bool empty() const { return terms->empty(); }
};

struct MergeTreeIndexTextDictionaryBlock
{
    ColumnPtr terms;
    PaddedPODArray<MarkInCompressedFile> marks_to_posting_lists;

    bool empty() const { return terms->empty(); }
};

struct MergeTreeIndexGranuleText final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleText();
    ~MergeTreeIndexGranuleText() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return sparse_index.empty(); }
    size_t memoryUsageBytes() const override { return 0; }

    BloomFilter bloom_filter;
    MergeTreeIndexTextSparseIndex sparse_index;
    std::vector<MergeTreeIndexTextDictionaryBlock> dictionary_blocks;
};

struct MergeTreeIndexGranuleTextWritable : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleTextWritable(
        BloomFilter bloom_filter_,
        std::vector<StringRef> terms_,
        std::vector<roaring::Roaring> posting_lists_,
        std::unique_ptr<Arena> arena_);

    ~MergeTreeIndexGranuleTextWritable() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return terms.empty(); }
    size_t memoryUsageBytes() const override { return 0; }

    BloomFilter bloom_filter;
    std::vector<StringRef> terms;
    std::vector<roaring::Roaring> posting_lists;
    std::unique_ptr<Arena> arena;
};

struct MergeTreeIndexTextGranuleBuilder
{
    explicit MergeTreeIndexTextGranuleBuilder(const BloomFilterParameters & bloom_filter_params_, TokenExtractorPtr token_extractor_);

    void addDocument(StringRef document);
    std::unique_ptr<MergeTreeIndexGranuleTextWritable> build();
    bool empty() const { return terms_map.empty(); }

    size_t current_row = 0;
    BloomFilter bloom_filter;
    TokenExtractorPtr token_extractor;
    std::vector<roaring::Roaring> posting_lists;

    using PostingListRawPtr = roaring::Roaring *;
    using TermsMap = HashMap<StringRef, PostingListRawPtr>;

    TermsMap terms_map;
    std::unique_ptr<Arena> arena;
};

struct MergeTreeIndexAggregatorText final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorText(String index_column_name_, BloomFilterParameters bloom_filter_params_, TokenExtractorPtr token_extractor_);
    ~MergeTreeIndexAggregatorText() override = default;

    bool empty() const override { return !granule_builder || granule_builder->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_column_name;
    BloomFilterParameters bloom_filter_params;
    TokenExtractorPtr token_extractor;
    std::optional<MergeTreeIndexTextGranuleBuilder> granule_builder;
};

}
