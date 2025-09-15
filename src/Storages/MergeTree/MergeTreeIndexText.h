#pragma once
#include <vector>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/IColumn.h>
#include <Formats/MarkInCompressedFile.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/logger_useful.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/ITokenExtractor.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{

struct MergeTreeIndexTextParams
{
    size_t dictionary_block_size = 0;
    size_t max_cardinality_for_embedded_postings = 0;
    size_t bloom_filter_bits_per_row = 0;
    size_t bloom_filter_num_hashes = 0;
};

using PostingList = roaring::Roaring;

struct PostingsSerialization
{
    enum Flags : UInt64
    {
        RawPostings = 1ULL << 0,
        EmbeddedPostings = 1ULL << 1,
    };

    static void serialize(UInt64 header, PostingList && postings, WriteBuffer & ostr);
    static PostingList deserialize(UInt64 header, UInt32 cardinality, ReadBuffer & istr);
};

struct TokenInfo
{
public:
    struct FuturePostings
    {
        UInt64 header = 0;
        UInt32 cardinality = 0;
        UInt64 offset_in_file = 0;
    };

    TokenInfo() : postings(FuturePostings{}) {}
    explicit TokenInfo(PostingList postings_) : postings(std::move(postings_)) {}
    explicit TokenInfo(FuturePostings postings_) : postings(std::move(postings_)) {}

    UInt32 getCardinality() const;
    bool empty() const { return getCardinality() == 0; }

    bool hasEmbeddedPostings() const { return std::holds_alternative<PostingList>(postings); }
    bool hasFuturePostings() const { return std::holds_alternative<FuturePostings>(postings); }

    PostingList & getEmbeddedPostings() { return std::get<PostingList>(postings); }
    FuturePostings & getFuturePostings() { return std::get<FuturePostings>(postings); }

    const PostingList & getEmbeddedPostings() const { return std::get<PostingList>(postings); }
    const FuturePostings & getFuturePostings() const { return std::get<FuturePostings>(postings); }

private:
    std::variant<PostingList, FuturePostings> postings;
};

struct DictionaryBlockBase
{
    ColumnPtr tokens;

    DictionaryBlockBase() = default;
    explicit DictionaryBlockBase(ColumnPtr tokens_) : tokens(std::move(tokens_)) {}

    bool empty() const;
    size_t size() const;

    size_t lowerBound(const StringRef & token) const;
    size_t upperBound(const StringRef & token) const;
    std::optional<size_t> binarySearch(const StringRef & token) const;
};

struct DictionarySparseIndex : public DictionaryBlockBase
{
    DictionarySparseIndex() = default;
    DictionarySparseIndex(ColumnPtr tokens_, ColumnPtr offsets_in_file_);
    UInt64 getOffsetInFile(size_t idx) const;

    ColumnPtr offsets_in_file;
};

struct DictionaryBlock : public DictionaryBlockBase
{
    DictionaryBlock() = default;
    DictionaryBlock(ColumnPtr tokens_, std::vector<TokenInfo> token_infos_);

    std::vector<TokenInfo> token_infos;
};

struct MergeTreeIndexGranuleText final : public IMergeTreeIndexGranule
{
public:
    using TokenInfosMap = absl::flat_hash_map<StringRef, TokenInfo>;

    explicit MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_);
    ~MergeTreeIndexGranuleText() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return bloom_filter_elements == 0; }
    size_t memoryUsageBytes() const override { return 0; }

    bool hasAnyTokenFromQuery(const TextSearchQuery & query) const;
    bool hasAllTokensFromQuery(const TextSearchQuery & query) const;

    const TokenInfosMap & getRemainingTokens() const { return remaining_tokens; }
    void resetAfterAnalysis();

private:
    void deserializeBloomFilter(ReadBuffer & istr);
    void deserializeSparseIndex(ReadBuffer & istr);

    void analyzeBloomFilter(const IMergeTreeIndexCondition & condition);
    void analyzeDictionary(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state);

    MergeTreeIndexTextParams params;
    size_t bloom_filter_elements = 0;
    BloomFilter bloom_filter;
    DictionarySparseIndex sparse_index;
    TokenInfosMap remaining_tokens;
};

using PostingListRawPtr = PostingList *;
using TokenToPostingsMap = StringHashMap<std::pair<PostingListRawPtr, roaring::BulkContext>>;
using SortedTokensAndPostings = std::vector<std::pair<StringRef, PostingList *>>;

struct MergeTreeIndexGranuleTextWritable : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleTextWritable(
        MergeTreeIndexTextParams params_,
        BloomFilter && bloom_filter_,
        SortedTokensAndPostings && tokens_and_postings_,
        TokenToPostingsMap && tokens_map_,
        std::list<PostingList> && posting_lists_,
        std::unique_ptr<Arena> && arena_);

    ~MergeTreeIndexGranuleTextWritable() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return tokens_and_postings.empty(); }
    size_t memoryUsageBytes() const override { return 0; }

    MergeTreeIndexTextParams params;
    BloomFilter bloom_filter;
    SortedTokensAndPostings tokens_and_postings;

    /// tokens_and_postings has references to data held in the fields below.
    TokenToPostingsMap tokens_map;
    std::list<PostingList> posting_lists;
    std::unique_ptr<Arena> arena;
};

struct MergeTreeIndexTextGranuleBuilder
{
    MergeTreeIndexTextGranuleBuilder(MergeTreeIndexTextParams params_, TokenExtractorPtr token_extractor_);

    void addDocument(StringRef document);
    std::unique_ptr<MergeTreeIndexGranuleTextWritable> build();
    bool empty() const { return current_row == 0; }
    void reset();

    MergeTreeIndexTextParams params;
    TokenExtractorPtr token_extractor;

    UInt64 current_row = 0;
    TokenToPostingsMap tokens_map;
    std::list<PostingList> posting_lists;
    std::unique_ptr<Arena> arena;
};

struct MergeTreeIndexAggregatorText final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorText(String index_column_name_, MergeTreeIndexTextParams params_, TokenExtractorPtr token_extractor_);
    ~MergeTreeIndexAggregatorText() override = default;

    bool empty() const override { return granule_builder.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_column_name;
    MergeTreeIndexTextParams params;
    TokenExtractorPtr token_extractor;
    MergeTreeIndexTextGranuleBuilder granule_builder;
};

class MergeTreeIndexText final : public IMergeTreeIndex
{
public:
    MergeTreeIndexText(
        const IndexDescription & index_,
        MergeTreeIndexTextParams params_,
        std::unique_ptr<ITokenExtractor> token_extractor_);

    ~MergeTreeIndexText() override = default;

    MergeTreeIndexSubstreams getSubstreams() const override;
    MergeTreeIndexFormat getDeserializedFormat(const IDataPartStorage & data_part_storage, const std::string & path_prefix) const override;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexTextParams params;
    std::unique_ptr<ITokenExtractor> token_extractor;
};

}
