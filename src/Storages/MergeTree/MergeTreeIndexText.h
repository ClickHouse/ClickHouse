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
    explicit PostingsSerialization(PostingListCodecPtr posting_list_codec_);

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

    void serialize(PostingListBuilder & postings, TokenPostingsInfo & info, size_t posting_list_block_size, WriteBuffer & ostr);
    void serialize(const PostingList & postings, TokenPostingsInfo & info, size_t posting_list_block_size, WriteBuffer & ostr);
    void serialize(const roaring::api::roaring_bitmap_t & postings, UInt64 header, WriteBuffer & ostr);
    PostingListPtr deserialize(ReadBuffer & istr, UInt64 header, UInt64 cardinality);
    PostingListCodecPtr getPostingListCodec() const { return posting_list_codec; }

private:
    PostingListCodecPtr posting_list_codec;

    /// Reusable buffers to avoid repeated heap allocations during deserialization.
    std::vector<UInt32> raw_postings_buffer;
    std::vector<char> deserialization_buffer;
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

    /// The majority of tokens have only one block,
    /// so use inlined vector to avoid heap allocations.
    absl::InlinedVector<UInt64, 1> offsets;
    absl::InlinedVector<RowsRange, 1> ranges;
    PostingListPtr embedded_postings;

    /// Returns indexes of posting list blocks to read for the given range of rows.
    std::vector<size_t> getBlocksToRead(const RowsRange & range) const;
    size_t bytesAllocated() const;
};

using TokenPostingsInfoPtr = std::shared_ptr<TokenPostingsInfo>;
using TokenToPostingsInfosMap = absl::flat_hash_map<String, TokenPostingsInfoPtr>;

/// Map from row ID to sorted token positions (0-based token sequence numbers) within that row.
using RowPositionsMap = absl::flat_hash_map<UInt32, std::vector<UInt32>>;

/// Builder for token position data during index writing.
/// Accumulates per-token, per-row position lists that correspond 1:1 with posting list entries.
/// Uses token sequence numbers (0-based) rather than byte offsets. [D-02, D-03]
struct TokenPositionsBuilder
{
    /// Add a token position: the token at `token_view` appeared at position `position` in row `row_id`.
    void add(std::string_view token_view, UInt32 row_id, UInt32 position)
    {
        data[token_view][row_id].push_back(position);
    }

    /// Get sorted positions for a given token across all rows.
    /// Returns a vector of (row_id, sorted positions) pairs, sorted by row_id.
    std::vector<std::pair<UInt32, std::vector<UInt32>>> getSortedPositions(std::string_view token_view) const
    {
        auto it = data.find(token_view);
        if (it == data.end())
            return {};

        std::vector<std::pair<UInt32, std::vector<UInt32>>> result;
        result.reserve(it->second.size());
        for (const auto & [row_id, positions] : it->second)
        {
            auto sorted_positions = positions;
            std::sort(sorted_positions.begin(), sorted_positions.end());
            result.emplace_back(row_id, std::move(sorted_positions));
        }
        std::sort(result.begin(), result.end(), [](const auto & a, const auto & b) { return a.first < b.first; });
        return result;
    }

    void clear() { data.clear(); }
    bool empty() const { return data.empty(); }

private:
    /// token -> (row_id -> positions)
    absl::flat_hash_map<std::string_view, RowPositionsMap> data;
};

/// A single row's position data: row ID and its sorted token positions.
using RowPositions = std::pair<UInt32, std::vector<UInt32>>;

/// Read-path data structure holding deserialized token position information.
/// Corresponds 1:1 with posting list data for a given token. [D-03]
struct TokenPositionsData
{
    /// Check if position data is available (non-empty).
    bool hasPositions() const { return !row_positions.empty(); }

    /// Get the position entries (row_id, positions) for this token.
    const std::vector<RowPositions> & getPositions() const { return row_positions; }

    std::vector<RowPositions> row_positions;
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
        PostingsSerialization & postings_serialization);

    static void serializeTokens(const ColumnString & tokens, WriteBuffer & ostr, TokensFormat format);
    static void serializeTokenInfo(WriteBuffer & ostr, const TokenPostingsInfo & token_info);
    static void serializeSparseIndex(const DictionarySparseIndex & sparse_index, WriteBuffer & ostr);

    /// Serializes token position data for a dictionary block of tokens. [D-07, D-15]
    /// Positions are written in the same token order as the posting lists (sorted alphabetically).
    /// Format per token per row: [count (VarUInt)] [first_pos (VarUInt)] [delta_1 (VarUInt)] ...
    static void serializePositions(
        const SortedTokensAndPostings & tokens_and_postings,
        const TokenPositionsBuilder & positions_builder,
        WriteBuffer & ostr,
        size_t block_begin,
        size_t block_end);

    /// Serializes position data for a single token. Used by the merge path. [D-07, D-09]
    static void serializePositions(const TokenPositionsData & positions, WriteBuffer & ostr);

    /// Deserializes position data for a single token from the .pos stream. [D-07]
    /// Symmetric to serializePositions: reads [num_rows] then per-row [row_id] [count] [first_pos] [deltas...].
    static TokenPositionsData deserializePositions(ReadBuffer & istr);

    static DictionarySparseIndex deserializeSparseIndex(ReadBuffer & istr);
    /// If postings_serialization is null, embedded postings are skipped.
    static TokenPostingsInfo deserializeTokenInfo(ReadBuffer & istr, PostingsSerialization * postings_serialization);
    static void skipTokenInfo(ReadBuffer & istr);

    /// Deserializes `TokenPostingsInfo` only for tokens at the given sorted indices,
    /// skipping postings for others. Returns a vector parallel to `matched_indices`.
    static std::vector<TokenPostingsInfo> deserializeTokenInfos(
        ReadBuffer & istr,
        size_t num_tokens,
        const std::vector<size_t> & matched_indices,
        PostingsSerialization & postings_serialization);

    /// Deserializes tokens from a dictionary block.
    /// Returns the tokens column and the tokens format.
    static std::pair<ColumnPtr, UInt64> deserializeTokens(ReadBuffer & istr);

    /// Deserializes a dictionary block into a new DictionaryBlock.
    /// If postings_serialization is null, embedded postings are skipped.
    static DictionaryBlock deserializeDictionaryBlock(ReadBuffer & istr, PostingsSerialization * postings_serialization);
};


/// Text index granule created on reading of the index.
struct MergeTreeIndexGranuleText final : public IMergeTreeIndexGranule
{
public:
    using TokenToPostingsMap = absl::flat_hash_map<std::string_view, PostingListPtr>;

    explicit MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_, PostingListCodecPtr posting_list_codec_);
    ~MergeTreeIndexGranuleText() override = default;

    const MergeTreeIndexTextParams & getParams() const { return params; }

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return is_empty; }
    size_t memoryUsageBytes() const override;

    bool hasAnyQueryTokens(const TextSearchQuery & query) const;
    bool hasAllQueryTokens(const TextSearchQuery & query) const;
    bool hasAllQueryTokensOrEmpty(const TextSearchQuery & query) const;
    bool hasPhraseQueryTokens(const TextSearchQuery & query) const;

    const TokenToPostingsInfosMap & getRemainingTokens() const { return remaining_tokens; }
    PostingListPtr getPostingsForRareToken(std::string_view token) const;
    void setCurrentRange(RowsRange range) { current_range = std::move(range); }
    const String & getIndexIdForCaches() const { return index_id_for_caches; }

    /// Returns true if position data was loaded from the .pos stream. [D-08]
    bool hasPositionData() const { return !token_positions.empty(); }
    /// Position data deserialized from the .pos stream, keyed by token string. [D-03]
    const absl::flat_hash_map<String, TokenPositionsData> & getTokenPositions() const { return token_positions; }

    static PostingListPtr readPostingsBlock(
        MergeTreeIndexReaderStream & stream,
        MergeTreeIndexDeserializationState & state,
        const TokenPostingsInfo & token_info,
        size_t block_idx,
        PostingsSerialization & postings_serialization,
        const String & index_id_for_caches);

private:
    /// Reads dictionary blocks and analyzes them for tokens.
    /// If positions_stream is non-null, also reads position data from the .pos stream. [D-08]
    void analyzeDictionary(MergeTreeIndexReaderStream & header_stream, MergeTreeIndexReaderStream & dictionary_stream, MergeTreeIndexDeserializationState & state, MergeTreeIndexReaderStream * positions_stream = nullptr);
    /// Fills tokens and their infos from the cache.
    /// Returns tokens that are not in the cache and need to be read from the dictionary file.
    std::vector<String> fillTokensFromCache(MergeTreeIndexDeserializationState & state);

    DictionarySparseIndexPtr loadSparseIndex(MergeTreeIndexReaderStream & header_stream, MergeTreeIndexDeserializationState & state);
    void readPostingsForRareTokens(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state);

    bool is_empty = true;
    /// If adding significantly large members here make sure to add them to memoryUsageBytes()
    MergeTreeIndexTextParams params;
    /// Tokens that are in the index granule after analysis.
    TokenToPostingsInfosMap remaining_tokens;
    /// Tokens with postings lists that have only one block.
    TokenToPostingsMap rare_tokens_postings;
    /// Current range of rows that is being processed. If set, mayBeTrueOnGranule returns more precise result.
    std::optional<RowsRange> current_range;
    /// Unique identifier for text index in the current data part.
    String index_id_for_caches;
    /// Serialization for the posting lists.
    PostingsSerialization postings_serialization;
    /// Deserialized token position data from the .pos stream. [D-03, D-08]
    /// Empty when the .pos stream is not present (old parts).
    absl::flat_hash_map<String, TokenPositionsData> token_positions;
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
        std::unique_ptr<Arena> && arena_,
        TokenPositionsBuilder && positions_builder_);

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
    /// Token position data for phrase query support. [D-03]
    TokenPositionsBuilder positions_builder;
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
    /// Token position builder for phrase query support. [D-02, D-03, D-04]
    TokenPositionsBuilder positions_builder;
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
    static DataTypePtr getNestedDataType(const DataTypePtr & data_type);

    MergeTreeIndexTextParams params;
    std::unique_ptr<ITokenizer> tokenizer;
    std::unique_ptr<IPostingListCodec> posting_list_codec;
    MergeTreeIndexTextPreprocessorPtr preprocessor;
};

}
