#pragma once
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Columns/IColumn.h>
#include <Formats/MarkInCompressedFile.h>
#include <Common/HashTable/HashMap.h>
#include "Interpreters/GinQueryString.h"
#include <Interpreters/BloomFilter.h>
#include <Interpreters/ITokenExtractor.h>
#include <absl/container/flat_hash_set.h>

namespace DB
{

class MergeTreeIndexReader;
class MergeTreeIndexConditionText;

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
    size_t size() const;
    MarkInCompressedFile getMark(size_t idx) const;

    size_t lowerBound(const StringRef & token) const;
    size_t upperBound(const StringRef & token) const;
    std::optional<size_t> binarySearch(const StringRef & token) const;
};

struct MergeTreeIndexGranuleText final : public IMergeTreeIndexGranule
{
public:
    explicit MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_);
    ~MergeTreeIndexGranuleText() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(IndexInputStreams & streams, IndexDeserializationState & state) override;

    bool empty() const override { return total_rows == 0; }
    size_t memoryUsageBytes() const override { return 0; }
    bool hasAllTokensFromQuery(const GinQueryString & query) const;
    void resetAfterAnalysis(bool may_be_true);

private:
    void deserializeBloomFilter(ReadBuffer & istr);
    void analyzeBloomFilter(const MergeTreeIndexConditionText & condition);
    void analyzeDictionary(MarkInCompressedFile begin_mark, TextSearchMode global_search_mode, IndexReaderStream & stream);

    MergeTreeIndexTextParams params;
    size_t total_rows = 0;
    BloomFilter bloom_filter;
    DictionaryBlock sparse_index;
    absl::flat_hash_map<StringRef, MarkInCompressedFile> remaining_tokens;
};

struct MergeTreeIndexGranuleTextWritable : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleTextWritable(
        size_t dictionary_block_size_,
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

    size_t dictionary_block_size;
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
    bool empty() const { return current_row == 0; }

    MergeTreeIndexTextParams params;
    TokenExtractorPtr token_extractor;

    using PostingListRawPtr = roaring::Roaring *;
    using TokensMap = HashMap<StringRef, PostingListRawPtr>;

    UInt64 current_row = 0;
    TokensMap tokens_map;
    std::list<roaring::Roaring> posting_lists;
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
    MergeTreeIndexFormat getDeserializedFormat(const IDataPartStorage & data_part_storage, const std::string & path_prefix) const override;
    bool hasHeavyGranules() const override { return true; }

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexTextParams params;
    std::unique_ptr<ITokenExtractor> token_extractor;
};

}
