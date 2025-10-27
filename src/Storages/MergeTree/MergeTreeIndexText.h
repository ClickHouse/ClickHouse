#pragma once
#include <vector>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/IColumn.h>
#include <Common/Logger.h>
#include <Formats/MarkInCompressedFile.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/logger_useful.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/ITokenExtractor.h>
#include <absl/container/flat_hash_map.h>

#include <roaring.hh>

namespace DB
{

/**
  * Implementation of inverted index for text search.
  *
  * A text index is a skip index that can have arbitrary granularity.
  * Granules are aggregated the same way as for other skip indexes
  * and dumped to the disk when the index reaches the desired granularity.
  *
  * Text index has three streams (files with data and marks for them):
  * - File with index granules (.idx)
  * - File with dictionary blocks (.dct)
  * - File with posting lists (.pst)
  *
  * Text index is supposed to be used with high cardinalities (128 by default).
  *
  * Each index granule accumulates tokens from all documents and collects the posting lists
  * (positions in the granule of documents that contain the token) for each token.
  * Tokens are sorted and split into blocks before the granule is finalized.
  * The block size is controlled by the index parameter 'dictionary_block_size'.
  * The first rows of each block form a sparse index (similar to the primary key of MergeTree).
  * All tokens are added to the bloom filter to have an ability to skip the granule quickly.
  *
  * Then index granule is written in the following way:
  * 1. Posting lists are dumped, and the offset in the file to the posting list for each token is saved.
  * 2. Posting lists are built and saved as Roaring Bitmaps. If the cardinality of the posting list is less than a threshold
  *    (index parameter 'max_cardinality_for_embedded_postings'), it is embedded into the dictionary.
  * 3. Then, dictionary blocks are dumped, and the offset in the dictionary file to the block is saved into the sparse index.
  *
  * The format of index granule:
  * - Bloom filter
  * - Sparse index - a mapping (first token in block -> offset in file to the beginning of the block).
  *
  * The format of sparse index:
  * - A binary serialized ColumnString with tokens (see SerializationString::serializeBinaryBulk)
  * - A binary serialized ColumnVector with offsets to dictionary blocks (see SerializationNumber::serializeBinaryBulk)
  *
  * Dictionary file consists of blocks. The format of dictionary block:
  * - Format of tokens (VarUInt). Currently only RawStrings format is supported.
  * - Number of tokens (VarUInt) in block.
  * - A binary serialized ColumnString with tokens.
  * - Information about posting lists for each token:
  *    1. Header of posting list (VarUInt) (see PostingsSerialization::Flags).
  *    2. Cardinality of token (VarUInt).
  *    3. Offset in file to the posting list (VarUInt) or embedded serialized posting list if EmbeddedPostings flag is set.
  *
  * If size of posting list is less than a threshold, it is serialized as raw values encoded as VarUInt.
  * Otherwise, the format is:
  * - Number of uncompressed bytes of the posting list (VarUInt).
  * - A binary serialized Roaring Bitmap (see Roaring::write and Roaring::read)
  */

struct MergeTreeIndexTextParams
{
    size_t dictionary_block_size = 0;
    size_t dictionary_block_frontcoding_compression = 1; /// enabled by default
    size_t max_cardinality_for_embedded_postings = 0;
    size_t bloom_filter_bits_per_row = 0;
    size_t bloom_filter_num_hashes = 0;
};

using PostingList = roaring::Roaring;

/// A struct for building a posting list with optimization for infrequent tokens.
/// Tokens with cardinality less than max_small_size are stored in a raw array allocated on the stack.
/// It avoids allocations of Roaring Bitmap for infrequent tokens without increasing the memory usage.
struct PostingListBuilder
{
public:
    using PostingListsHolder = std::list<PostingList>;
    using PostingListWithContext = std::pair<PostingList *, roaring::BulkContext>;

    /// sizeof(PostingListWithContext) == 24 bytes.
    /// Use small container of the same size to reuse this memory.
    static constexpr size_t max_small_size = 6;
    using SmallContainer = std::array<UInt32, max_small_size>;

    PostingListBuilder() : small_size(0) {}

    /// Adds a value to small array or to the large Roaring Bitmap.
    /// If small array is converted to Roaring Bitmap after adding a value,
    /// posting list is created in the postings_holder and reference to it is saved.
    void add(UInt32 value, PostingListsHolder & postings_holder);

    size_t size() const { return isSmall() ? small_size : large.first->cardinality(); }
    bool isSmall() const { return small_size < max_small_size; }
    SmallContainer & getSmall() { return small; }
    PostingList & getLarge() const { return *large.first; }

private:
    union
    {
        SmallContainer small;
        PostingListWithContext large;
    };

    UInt8 small_size;
};

struct PostingsSerialization
{
    enum Flags : UInt64
    {
        /// If set, the posting list is serialized as raw UInt32 values encoded as VarUInt.
        /// The minimal size of serialized Roaring Bitmap is 48 bytes, it doesn't make sense to use it for cardinality less than 16.
        RawPostings = 1ULL << 0,
        /// If set, the posting list is embedded into the dictionary block to avoid additional random reads from disk.
        EmbeddedPostings = 1ULL << 1,
    };

    static UInt64 serialize(UInt64 header, PostingListBuilder && postings, WriteBuffer & ostr);
    static PostingList deserialize(UInt64 header, UInt32 cardinality, ReadBuffer & istr);
};

/// Stores information about posting list for a token.
/// It can be either a future posting list (when the posting list is written in a separate file)
/// or an embedded posting list (when the posting list is embedded into the dictionary block).
struct TokenPostingsInfo
{
public:
    /// Information required to read the posting list.
    struct FuturePostings
    {
        UInt64 header = 0;
        UInt32 cardinality = 0;
        UInt64 offset_in_file = 0;
    };

    TokenPostingsInfo() : postings(FuturePostings{}) {}
    explicit TokenPostingsInfo(PostingList postings_) : postings(std::move(postings_)) {}
    explicit TokenPostingsInfo(FuturePostings postings_) : postings(std::move(postings_)) {}

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
    DictionaryBlock(ColumnPtr tokens_, std::vector<TokenPostingsInfo> token_infos_);

    std::vector<TokenPostingsInfo> token_infos;
};

/// Text index granule created on reading of the index.
struct MergeTreeIndexGranuleText final : public IMergeTreeIndexGranule
{
public:
    using TokenToPostingsInfosMap = absl::flat_hash_map<StringRef, TokenPostingsInfo>;

    explicit MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_);
    ~MergeTreeIndexGranuleText() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return num_tokens == 0; }
    size_t memoryUsageBytes() const override;

    bool hasAnyTokenFromQuery(const TextSearchQuery & query) const;
    bool hasAllTokensFromQuery(const TextSearchQuery & query) const;

    const TokenToPostingsInfosMap & getRemainingTokens() const { return remaining_tokens; }
    void resetAfterAnalysis();

private:
    void deserializeBloomFilter(ReadBuffer & istr);
    void deserializeSparseIndex(ReadBuffer & istr);

    /// Analyzes bloom filters. Removes tokens that are not present in the bloom filter.
    void analyzeBloomFilter(const IMergeTreeIndexCondition & condition);
    /// Reads dictionary blocks and analyzes them for tokens remaining after bloom filter analysis.
    void analyzeDictionary(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state);

    MergeTreeIndexTextParams params;
    size_t num_tokens = 0;
    BloomFilter bloom_filter;
    DictionarySparseIndex sparse_index;
    /// Tokens that are in the index granule after analysis.
    TokenToPostingsInfosMap remaining_tokens;
};

/// Save BulkContext to optimize consecutive insertions into the posting list.
using TokenToPostingsMap = StringHashMap<PostingListBuilder>;
using SortedTokensAndPostings = std::vector<std::pair<StringRef, PostingListBuilder *>>;

/// Text index granule created on writing of the index.
/// It differs from MergeTreeIndexGranuleText because it
/// is used only when building the index and stores different data structures.
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
    /// Pointers to tokens and posting lists in the granule.
    SortedTokensAndPostings tokens_and_postings;

    /// tokens_and_postings has references to data held in the fields below.
    TokenToPostingsMap tokens_map;
    std::list<PostingList> posting_lists;
    std::unique_ptr<Arena> arena;
    LoggerPtr logger;
};

struct MergeTreeIndexTextGranuleBuilder
{
    MergeTreeIndexTextGranuleBuilder(MergeTreeIndexTextParams params_, TokenExtractorPtr token_extractor_);

    /// Extracts tokens from the document and adds them to the granule.
    void addDocument(StringRef document);
    void incrementCurrentRow() { ++current_row; }

    std::unique_ptr<MergeTreeIndexGranuleTextWritable> build();
    bool empty() const { return current_row == 0; }
    void reset();

    MergeTreeIndexTextParams params;
    TokenExtractorPtr token_extractor;

    UInt64 current_row = 0;
    /// Pointers to posting lists for each token.
    TokenToPostingsMap tokens_map;
    /// Holder of posting lists. std::list is used to preserve the stability of pointers to posting lists.
    std::list<PostingList> posting_lists;
    /// Keys may be serialized into arena (see ArenaKeyHolder).
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

    bool supportsReadingOnParallelReplicas() const override { return true; }
    MergeTreeIndexSubstreams getSubstreams() const override;
    MergeTreeIndexFormat getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & path_prefix) const override;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexTextParams params;
    std::unique_ptr<ITokenExtractor> token_extractor;
};

}
