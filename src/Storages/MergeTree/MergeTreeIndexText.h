#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/IColumn.h>
#include <Common/Logger.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/logger_useful.h>
#include <Formats/MarkInCompressedFile.h>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <base/types.h>

#include <vector>

#include <roaring/roaring.hh>

namespace DB
{

/**
  * Implementation of inverted index for text search.
  *
  * A text index is a skip index that is always calculated on the whole and has infinite granularity.
  * Granules are aggregated the same way as for other skip indexes
  * Unlike other skip indexes, text index can be merged instead of rebuilt on merge of the data parts.
  *
  * Text index has three streams (files with data and marks for them):
  * - File with index granules (.idx)
  * - File with dictionary blocks (.dct)
  * - File with posting lists (.pst)
  *
  * Index granule accumulates tokens from all documents and collects the posting lists
  * (positions in the granule of documents that contain the token) for each token.
  * Tokens are sorted and split into blocks before the granule is finalized.
  * The block size is controlled by the index parameter 'dictionary_block_size'.
  * The first rows of each block form a sparse index (similar to the primary key of MergeTree).
  *
  * Then index granule is written in the following way:
  * 1. Posting lists are dumped in blocks of size 'posting_list_block_size'.
  * 2. Offsets in the file to the posting list blocks along with min-max range of the block for each token are saved.
  * 3. Posting lists are built and saved as Roaring Bitmaps.
  * 4. If the cardinality of the posting list is less than a threshold it is embedded into the dictionary.
  * 5. Dictionary blocks are dumped, and the offset in the dictionary file to the block is saved into the sparse index.
  *
  * The format of index granule:
  * - Sparse index - a mapping (first token in block -> offset in file to the beginning of the block).
  *
  * The format of sparse index:
  * - A binary serialized ColumnString with tokens (see SerializationString::serializeBinaryBulk)
  * - A binary serialized ColumnVector with offsets to dictionary blocks (see SerializationNumber::serializeBinaryBulk)
  *
  * Dictionary file consists of blocks. The format of dictionary block:
  * - Format of tokens (VarUInt). Currently raw and front-coded string formats are supported.
  * - Number of tokens (VarUInt) in block.
  * - A binary serialized ColumnString with tokens.
  * - Information about posting lists for each token:
  *    1. Header of posting list (VarUInt) (see PostingsSerialization::Flags).
  *    2. Cardinality of token (VarUInt).
  *    3. a) If EmbeddedPostings flag is set, posting list embedded into the dictionary block.
  *       b) Otherwise, number of blocks of the posting list (VarUInt), if SingleBlock flag is not set.
  *       c) For each posting list block, offset in file to the block and min-max range of the block. All numbers are encoded as VarUInt.
  *
  * If size of posting list is less than a threshold, it is serialized as raw values encoded as VarUInts.
  * Otherwise, the format is:
  * - Number of uncompressed bytes of the posting list (VarUInt).
  * - A binary serialized Roaring Bitmap (see Roaring::write and Roaring::read)
  */

class IPostingListCodec;
using PostingListCodecPtr = const IPostingListCodec *;

struct MergeTreeIndexTextParams
{
    size_t dictionary_block_size = 0;
    size_t dictionary_block_frontcoding_compression = 1;
    size_t posting_list_block_size = 1024 * 1024;
    ASTPtr preprocessor;
};

using PostingList = roaring::Roaring;
using PostingListPtr = std::shared_ptr<PostingList>;

/// A struct for building a posting list with optimization for infrequent tokens.
/// Tokens with cardinality less than max_small_size are stored in a raw array allocated on the stack.
/// It avoids allocations of Roaring Bitmap for infrequent tokens without increasing the memory usage.
struct PostingListBuilder
{
public:
    using PostingListsHolder = std::list<PostingList>;

    /// sizeof(PostingListWithContext) == 24 bytes.
    /// Use small container of the same size to reuse this memory.
    static constexpr size_t max_small_size = 6;
    using SmallContainer = std::array<UInt32, max_small_size>;

    PostingListBuilder() : small_size(0) {}
    explicit PostingListBuilder(PostingList * posting_list);

    /// Adds a value to small array or to the large Roaring Bitmap.
    /// If small array is converted to Roaring Bitmap after adding a value,
    /// posting list is created in the postings_holder and reference to it is saved.
    void add(UInt32 value, PostingListsHolder & postings_holder);

    size_t size() const { return isSmall() ? small_size : large.postings->cardinality(); }
    bool isEmpty() const { return size() == 0; }
    bool isSmall() const { return small_size < max_small_size; }
    bool isLarge() const { return !isSmall(); }
    UInt32 minimum() const { return isSmall() ? small[0] : large.postings->minimum(); }
    UInt32 maximum() const { return isSmall() ? small[small_size - 1] : large.postings->maximum(); }

    SmallContainer & getSmall() { return small; }
    const SmallContainer & getSmall() const { return small; }
    PostingList & getLarge() const { return *large.postings; }

private:
    struct PostingListWithContext
    {
        PostingList * postings;
        roaring::BulkContext context;
    };

    union
    {
        SmallContainer small;
        PostingListWithContext large;
    };

    UInt8 small_size;
};

/// Save BulkContext to optimize consecutive insertions into the posting list.
using TokenToPostingsBuilderMap = StringHashMap<PostingListBuilder>;
using SortedTokensAndPostings = std::vector<std::pair<std::string_view, PostingListBuilder *>>;
struct TokenPostingsInfo;

struct PostingsSerialization
{
    enum Flags : UInt64
    {
        /// If set, the posting list is serialized as raw UInt32 values encoded as VarUInt.
        /// The minimal size of serialized Roaring Bitmap is 48 bytes,
        /// it doesn't make sense to use it for cardinality less than MAX_CARDINALITY_FOR_RAW_POSTINGS.
        RawPostings = 1ULL << 0,
        /// If set, the posting list is embedded into the dictionary block to avoid additional random reads from disk.
        EmbeddedPostings = 1ULL << 1,
        /// If unset, the number of blocks is stored as an additional VarUInt.
        SingleBlock = 1ULL << 2,
        /// If set, the posting list is encoded using posting_list_codec.
        IsCompressed = 1ULL << 3,
    };

    static void serialize(PostingListBuilder & postings, TokenPostingsInfo & info, size_t posting_list_block_size, PostingListCodecPtr posting_list_codec, WriteBuffer & ostr);
    static void serialize(const PostingList & postings, TokenPostingsInfo & info, size_t posting_list_block_size, PostingListCodecPtr posting_list_codec, WriteBuffer & ostr);
    static void serialize(const roaring::api::roaring_bitmap_t & postings, UInt64 header, WriteBuffer & ostr);
    static PostingListPtr deserialize(ReadBuffer & istr, UInt64 header, UInt64 cardinality, PostingListCodecPtr posting_list_codec);
};

/// Closed range of rows.
struct RowsRange
{
    size_t begin;
    size_t end;

    RowsRange() = default;
    RowsRange(size_t begin_, size_t end_) : begin(begin_), end(end_) {}

    bool intersects(const RowsRange & other) const;
};

/// Stores information about posting list for a token.
struct TokenPostingsInfo
{
    UInt64 header = 0;
    UInt32 cardinality = 0;
    std::vector<UInt64> offsets;
    std::vector<RowsRange> ranges;
    PostingListPtr embedded_postings;

    /// Returns indexes of posting list blocks to read for the given range of rows.
    std::vector<size_t> getBlocksToRead(const RowsRange & range) const;
};

struct DictionaryBlockBase
{
    ColumnPtr tokens;

    DictionaryBlockBase() = default;
    explicit DictionaryBlockBase(ColumnPtr tokens_) : tokens(std::move(tokens_)) {}

    bool empty() const;
    size_t size() const;

    size_t upperBound(std::string_view token) const;
};

struct DictionaryBlock : public DictionaryBlockBase
{
    DictionaryBlock() = default;
    DictionaryBlock(ColumnPtr tokens_, std::vector<TokenPostingsInfo> token_infos_, UInt64 tokens_format_);

    std::vector<TokenPostingsInfo> token_infos;
    UInt64 tokens_format = 0;
};

struct DictionarySparseIndex : public DictionaryBlockBase
{
    DictionarySparseIndex() = default;
    DictionarySparseIndex(ColumnPtr tokens_, ColumnPtr offsets_in_file_);
    UInt64 getOffsetInFile(size_t idx) const;
    size_t memoryUsageBytes() const;

    ColumnPtr offsets_in_file;
};

using DictionarySparseIndexPtr = std::shared_ptr<DictionarySparseIndex>;

struct TextIndexSerialization
{
    enum class SparseIndexVersion
    {
        Initial = 0,
    };

    enum class TokensFormat : UInt64
    {
        RawStrings = 0,
        FrontCodedStrings = 1
    };

    static TokenPostingsInfo serializePostings(
        PostingListBuilder & postings,
        MergeTreeIndexWriterStream & postings_stream,
        const MergeTreeIndexTextParams & params,
        PostingListCodecPtr posting_list_codec);

    static void serializeTokens(const ColumnString & tokens, WriteBuffer & ostr, TokensFormat format);
    static void serializeTokenInfo(WriteBuffer & ostr, const TokenPostingsInfo & token_info);
    static void serializeSparseIndex(const DictionarySparseIndex & sparse_index, WriteBuffer & ostr);

    static DictionarySparseIndex deserializeSparseIndex(ReadBuffer & istr);
    static TokenPostingsInfo deserializeTokenInfo(ReadBuffer & istr, PostingListCodecPtr posting_list_codec);
    static DictionaryBlock deserializeDictionaryBlock(ReadBuffer & istr, PostingListCodecPtr posting_list_codec);
};


/// Text index granule created on reading of the index.
struct MergeTreeIndexGranuleText final : public IMergeTreeIndexGranule
{
public:
    using TokenToPostingsInfosMap = absl::flat_hash_map<std::string_view, TokenPostingsInfo>;
    using TokenToPostingsMap = absl::flat_hash_map<std::string_view, PostingListPtr>;

    explicit MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_, PostingListCodecPtr posting_list_codec_);
    ~MergeTreeIndexGranuleText() override = default;

    const MergeTreeIndexTextParams & getParams() const { return params; }
    PostingListCodecPtr getPostingListCodec() const { return posting_list_codec; }

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return sparse_index->empty(); }
    size_t memoryUsageBytes() const override;

    bool hasAnyQueryTokens(const TextSearchQuery & query) const;
    bool hasAllQueryTokens(const TextSearchQuery & query) const;
    bool hasAllQueryTokensOrEmpty(const TextSearchQuery & query) const;

    const TokenToPostingsInfosMap & getRemainingTokens() const { return remaining_tokens; }
    PostingListPtr getPostingsForRareToken(std::string_view token) const;
    void setCurrentRange(RowsRange range) { current_range = std::move(range); }
    void resetAfterAnalysis();

    static PostingListPtr readPostingsBlock(
        MergeTreeIndexReaderStream & stream,
        MergeTreeIndexDeserializationState & state,
        const TokenPostingsInfo & token_info,
        size_t block_idx,
        PostingListCodecPtr posting_list_codec);

private:
    void readSparseIndex(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state);
    /// Reads dictionary blocks and analyzes them for tokens.
    void analyzeDictionary(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state);
    void readPostingsForRareTokens(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state);

    /// If adding significantly large members here make sure to add them to memoryUsageBytes()
    MergeTreeIndexTextParams params;
    /// Header of the text index contains the number of tokens and sparse index.
    DictionarySparseIndexPtr sparse_index;
    /// Tokens that are in the index granule after analysis.
    TokenToPostingsInfosMap remaining_tokens;
    /// Tokens with postings lists that have only one block.
    TokenToPostingsMap rare_tokens_postings;
    /// Current range of rows that is being processed. If set, mayBeTrueOnGranule returns more precise result.
    std::optional<RowsRange> current_range;
    PostingListCodecPtr posting_list_codec;
};

/// Text index granule created on writing of the index.
/// It differs from MergeTreeIndexGranuleText because it
/// is used only when building the index and stores different data structures.
struct MergeTreeIndexGranuleTextWritable : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleTextWritable(
        MergeTreeIndexTextParams params_,
        PostingListCodecPtr posting_list_codec_,
        SortedTokensAndPostings && tokens_and_postings_,
        TokenToPostingsBuilderMap && tokens_map_,
        std::list<PostingList> && posting_lists_,
        std::unique_ptr<Arena> && arena_);

    ~MergeTreeIndexGranuleTextWritable() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return tokens_and_postings.empty(); }
    size_t memoryUsageBytes() const override;

    /// If adding significantly large members here make sure to add them to memoryUsageBytes()
    MergeTreeIndexTextParams params;
    PostingListCodecPtr posting_list_codec = nullptr;
    /// Pointers to tokens and posting lists in the granule.
    SortedTokensAndPostings tokens_and_postings;
    /// tokens_and_postings has references to data held in the fields below.
    TokenToPostingsBuilderMap tokens_map;
    std::list<PostingList> posting_lists;
    std::unique_ptr<Arena> arena;
    LoggerPtr logger;
};

struct ITokenizer;
using TokenizerPtr = const ITokenizer *;

struct MergeTreeIndexTextGranuleBuilder
{
    MergeTreeIndexTextGranuleBuilder(
        MergeTreeIndexTextParams params_,
        TokenizerPtr tokenizer_,
        PostingListCodecPtr posting_list_codec_);

    /// Extracts tokens from the document and adds them to the granule.
    void addDocument(std::string_view document);
    void incrementCurrentRow();
    void setCurrentRow(size_t row) { current_row = row; }

    std::unique_ptr<MergeTreeIndexGranuleTextWritable> build();
    bool empty() const { return is_empty; }
    void reset();

    MergeTreeIndexTextParams params;
    TokenizerPtr tokenizer;
    PostingListCodecPtr posting_list_codec;

    bool is_empty = true;
    UInt64 current_row = 0;
    UInt64 num_processed_tokens = 0;
    /// Pointers to posting lists for each token.
    TokenToPostingsBuilderMap tokens_map;
    /// Holder of posting lists. std::list is used to preserve the stability of pointers to posting lists.
    std::list<PostingList> posting_lists;
    /// Keys may be serialized into arena (see ArenaKeyHolder).
    std::unique_ptr<Arena> arena;
};

class MergeTreeIndexTextPreprocessor;
using MergeTreeIndexTextPreprocessorPtr = std::shared_ptr<MergeTreeIndexTextPreprocessor>;

struct MergeTreeIndexAggregatorText final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorText(
        String index_column_name_,
        MergeTreeIndexTextParams params_,
        TokenizerPtr tokenizer_,
        PostingListCodecPtr posting_list_codec_,
        MergeTreeIndexTextPreprocessorPtr preprocessor_);

    ~MergeTreeIndexAggregatorText() override = default;

    bool empty() const override { return granule_builder.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;
    void setCurrentRow(size_t row) { granule_builder.setCurrentRow(row); }
    UInt64 getNumProcessedTokens() const { return granule_builder.num_processed_tokens; }

    String index_column_name;
    MergeTreeIndexTextParams params;
    TokenizerPtr tokenizer;
    PostingListCodecPtr posting_list_codec;
    MergeTreeIndexTextGranuleBuilder granule_builder;
    MergeTreeIndexTextPreprocessorPtr preprocessor;
};

class MergeTreeIndexText final : public IMergeTreeIndex
{
public:
    MergeTreeIndexText(
        const IndexDescription & index_,
        MergeTreeIndexTextParams params_,
        std::unique_ptr<ITokenizer> tokenizer_,
        std::unique_ptr<IPostingListCodec> posting_list_codec_);

    ~MergeTreeIndexText() override = default;

    MergeTreeIndexTextParams getParams() const { return params; }
    bool isTextIndex() const override { return true; }

    MergeTreeIndexSubstreams getSubstreams() const override;
    MergeTreeIndexFormat getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & path_prefix) const override;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    PostingListCodecPtr getPostingListCodec() const { return posting_list_codec.get(); }

    MergeTreeIndexTextParams params;
    std::unique_ptr<ITokenizer> tokenizer;
    std::unique_ptr<IPostingListCodec> posting_list_codec;
    MergeTreeIndexTextPreprocessorPtr preprocessor;
};

}
