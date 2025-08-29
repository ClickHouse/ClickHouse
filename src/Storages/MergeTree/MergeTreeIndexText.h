#pragma once
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Columns/IColumn.h>
#include <Formats/MarkInCompressedFile.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilterText.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

struct MergeTreeIndexTextParams
{
    size_t dictionary_block_size = 0;
    size_t bloom_filter_bits_per_row = 0;
    size_t bloom_filter_num_hashes = 0;
};

struct DictionaryBlock
{
    DictionaryBlock() = default;
    DictionaryBlock(ColumnPtr tokens_, ColumnPtr marks_);

    ColumnPtr tokens;
    ColumnPtr marks;

    bool empty() const;
};

struct MergeTreeIndexGranuleText final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_);
    ~MergeTreeIndexGranuleText() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return sparse_index.empty(); }
    size_t memoryUsageBytes() const override { return 0; }

    MergeTreeIndexTextParams params;
    std::optional<BloomFilter> bloom_filter;
    DictionaryBlock sparse_index;
    std::vector<DictionaryBlock> dictionary_blocks;
};

struct MergeTreeIndexGranuleTextWritable : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleTextWritable(
        size_t total_rows_,
        BloomFilter bloom_filter_,
        std::vector<StringRef> tokens_,
        std::vector<roaring::Roaring> posting_lists_,
        std::unique_ptr<Arena> arena_);

    ~MergeTreeIndexGranuleTextWritable() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void serializeBinaryWithMultipleStreams(IndexOutputStreams & streams) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return tokens.empty(); }
    size_t memoryUsageBytes() const override { return 0; }

    size_t total_rows;
    BloomFilter bloom_filter;
    std::vector<StringRef> tokens;
    std::vector<roaring::Roaring> posting_lists;
    std::unique_ptr<Arena> arena;
};

struct MergeTreeIndexTextGranuleBuilder
{
    explicit MergeTreeIndexTextGranuleBuilder(MergeTreeIndexTextParams params_, TokenExtractorPtr token_extractor_);

    void addDocument(StringRef document);
    std::unique_ptr<MergeTreeIndexGranuleTextWritable> build();
    bool empty() const { return tokens_map.empty(); }

    MergeTreeIndexTextParams params;
    TokenExtractorPtr token_extractor;

    using PostingListRawPtr = roaring::Roaring *;
    using TokensMap = HashMap<StringRef, PostingListRawPtr>;

    UInt64 current_row = 0;
    TokensMap tokens_map;
    std::vector<roaring::Roaring> posting_lists;
    std::unique_ptr<Arena> arena;
};

struct MergeTreeIndexAggregatorText final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorText(String index_column_name_, MergeTreeIndexTextParams params_, TokenExtractorPtr token_extractor_);
    ~MergeTreeIndexAggregatorText() override = default;

    bool empty() const override { return !granule_builder || granule_builder->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_column_name;
    MergeTreeIndexTextParams params;
    TokenExtractorPtr token_extractor;
    std::optional<MergeTreeIndexTextGranuleBuilder> granule_builder;
};

class MergeTreeIndexText final : public IMergeTreeIndex
{
public:
    MergeTreeIndexText(
        const IndexDescription & index_,
        MergeTreeIndexTextParams params_,
        std::unique_ptr<ITokenExtractor> token_extractor_);

    ~MergeTreeIndexText() override = default;

    IndexSubstreams getSubstreams() const override;
    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexTextParams params;
    std::unique_ptr<ITokenExtractor> token_extractor;
};

}
