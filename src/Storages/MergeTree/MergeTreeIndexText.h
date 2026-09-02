#pragma once

#include <Core/SettingsEnums.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/IColumn.h>
#include <Common/BitPackedStringArray.h>
#include <Common/BitPackedUInt64Array.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/TextIndexPositionData.h>
#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <Formats/MarkInCompressedFile.h>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <base/types.h>

#include <concepts>
#include <span>
#include <variant>
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
  * Text index has three streams (files with data and marks for them), plus a fourth with 'support_phrase_search':
  * - File with index granules (.idx)
  * - File with dictionary blocks (.dct)
  * - File with posting lists (.pst)
  * - File with token positions (.pos), one blob per token (see TextIndexBlockedPositionsCodec)
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
  *    3. If HasPositions flag is set, the token's offset and byte length in .pos (VarUInt each).
  *    4. a) If EmbeddedPostings flag is set, posting list embedded into the dictionary block.
  *       b) Otherwise, number of blocks of the posting list (VarUInt), if SingleBlock flag is not set.
  *       c) For each posting list block, offset in file to the block and min-max range of the block. All numbers are encoded as VarUInt.
  *
  * If size of posting list is less than a threshold, it is serialized as raw values encoded as VarUInts.
  * Otherwise, the format is:
  * - Number of uncompressed bytes of the posting list (VarUInt).
  * - A binary serialized Roaring Bitmap (see Roaring::write and Roaring::read)
  */

using PostingListCodecPtr = std::unique_ptr<IPostingListCodec>;

struct MergeTreeIndexTextParams
{
    size_t dictionary_block_size = 0;
    size_t dictionary_block_frontcoding_compression = 1;
    size_t posting_list_block_size = 1024 * 1024;
    size_t positions = 0;
    UInt8 positions_codec = static_cast<UInt8>(TextIndexPositionCodec::Encoding::BlockedPfor);
    ASTPtr preprocessor;
    ASTPtr postprocessor;
    MergeTreeTextIndexSerializationVersion serialization_version = MergeTreeTextIndexSerializationVersion::V0_Initial;
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

    size_t size() const
    {
        chassert(!isFiltered());
        return isSmall() ? small_size : large.postings->cardinality();
    }
    bool isEmpty() const { return size() == 0; }
    bool isSmall() const { return small_size < max_small_size; }
    bool isLarge() const { return !isSmall(); }

    /// A filtered entry holds no postings; only isFiltered and clearFiltered may be called on it.
    void markFiltered() { small_size = filtered_flag; }
    bool isFiltered() const { return small_size == filtered_flag; }
    void clearFiltered() { small_size = 0; }

    UInt32 minimum() const
    {
        chassert(!isEmpty());
        return isSmall() ? small[0] : large.postings->minimum();
    }

    UInt32 maximum() const
    {
        chassert(!isEmpty());
        return isSmall() ? small[small_size - 1] : large.postings->maximum();
    }

    SmallContainer & getSmall() { chassert(!isFiltered()); return small; }
    const SmallContainer & getSmall() const { chassert(!isFiltered()); return small; }
    PostingList & getLarge() const { chassert(!isFiltered()); return *large.postings; }

private:
    struct PostingListWithContext
    {
        PostingList * postings;
        roaring::BulkContext context;
    };

    union
    {
        SmallContainer small{};
        PostingListWithContext large;
    };

    static constexpr UInt8 filtered_flag = 0xFF;
    UInt8 small_size;
};

/// Save BulkContext to optimize consecutive insertions into the posting list.
using TokenToPostingsBuilderMap = StringHashMap<PostingListBuilder>;
/// A token paired with its posting/position builder views.
struct SortedToken
{
    std::string_view token;
    PostingListBuilder * postings = nullptr;
    PositionListBuilder * positions = nullptr; /// nullptr unless text index has `support_phrase_search` enabled
};
using SortedTokens = std::vector<SortedToken>;
struct TokenPostingsInfo;

/// Posting lists up to this cardinality are serialized as raw VarUInt values:
/// the minimal size of a serialized Roaring Bitmap is 48 bytes, so tiny lists don't use it.
static constexpr UInt64 MAX_CARDINALITY_FOR_RAW_POSTINGS = 12;
/// Posting lists up to this cardinality are embedded into the dictionary block
/// to avoid additional random reads from disk.
static constexpr UInt64 MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS = 6;

static_assert(MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS <= MAX_CARDINALITY_FOR_RAW_POSTINGS, "MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS must be less or equal to MAX_CARDINALITY_FOR_RAW_POSTINGS");
static_assert(PostingListBuilder::max_small_size <= MAX_CARDINALITY_FOR_RAW_POSTINGS, "max_small_size must be less than or equal to MAX_CARDINALITY_FOR_RAW_POSTINGS");

struct PostingsSerialization
{
    PostingsSerialization(PostingListCodecPtr posting_list_codec_, MergeTreeTextIndexSerializationVersion serialization_version_);

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
        /// If set, each compressed segment has a V2 Index Section with per-block metadata
        /// (last_row_id + relative_offset arrays) enabling binary-search in PostingListCursor.
        HasBlockIndex = 1ULL << 4,
        /// If set, the token has positional data in the .pos file.
        HasPositions = 1ULL << 5,
    };

    /// Serializes a roaring bitmap as a portable serialized bitmap.
    void serializeBitmap(const roaring::api::roaring_bitmap_t & postings, WriteBuffer & ostr);
    /// Serializes a posting lists using the posting list codec.
    void serializeCompressed(const PostingList & postings, TokenPostingsInfo & info, size_t posting_list_block_size, WriteBuffer & ostr);
    /// Serializes a plain array of row ids as VarUInt values.
    static void serializeRaw(std::span<const UInt32> postings, WriteBuffer & ostr);

    /// Returns the row ids of a posting list as a plain array of ascending unique values.
    /// A large container is converted into the reusable buffer, so the returned span is valid until the next call.
    std::span<const UInt32> toRawPostings(const PostingListBuilder & postings);
    std::span<const UInt32> toRawPostings(std::span<const UInt32> postings) const { return postings; }

    PostingListPtr deserializeToBitmap(ReadBuffer & istr, UInt64 header, UInt64 cardinality);
    void deserializeToArray(ReadBuffer & istr, UInt64 header, UInt64 cardinality, PaddedPODArray<UInt32> & row_ids);
    const IPostingListCodec * getPostingListCodec() const { return posting_list_codec.get(); }

private:
    const IPostingListCodec & resolveCodec(UInt64 header);

    PostingListCodecPtr posting_list_codec;
    MergeTreeTextIndexSerializationVersion serialization_version;

    /// Reusable buffers to avoid repeated heap allocations during serialization/deserialization.
    PaddedPODArray<UInt32> raw_postings_buffer;
    PaddedPODArray<char> raw_data_buffer;
};

/// Closed range of rows.
struct RowsRange
{
    size_t begin;
    size_t end;

    RowsRange() = default;
    RowsRange(size_t begin_, size_t end_) : begin(begin_), end(end_) {}

    bool intersects(const RowsRange & other) const;
    std::optional<RowsRange> intersectWith(const RowsRange & other) const;
    RowsRange unionWith(const RowsRange & other) const;
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
    absl::InlinedVector<UInt32, MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS> embedded_postings;

    /// Position data offset in the .pos file
    UInt64 position_offset = 0;
    /// Byte length of the position blob, so readers bound it by the token's extent, not the file's.
    UInt64 position_bytes = 0;

    /// Returns indexes of posting list blocks to read for the given range of rows.
    std::vector<size_t> getBlocksToRead(const RowsRange & range) const;
    size_t bytesAllocated() const;
};

using TokenPostingsInfoPtr = std::shared_ptr<TokenPostingsInfo>;
using TokenToPostingsInfosMap = absl::flat_hash_map<String, TokenPostingsInfoPtr>;

struct DictionaryBlock
{
    DictionaryBlock() = default;
    DictionaryBlock(ColumnPtr tokens_, std::vector<TokenPostingsInfo> token_infos_, UInt64 tokens_format_);

    bool empty() const;
    size_t size() const;

    ColumnPtr tokens;
    std::vector<TokenPostingsInfo> token_infos;
    UInt64 tokens_format = 0;
};

class DictionarySparseIndex
{
public:
    DictionarySparseIndex() = default;
    DictionarySparseIndex(ColumnPtr tokens_, ColumnPtr offsets_in_file_);

    bool empty() const { return size() == 0; }
    size_t size() const;
    size_t upperBound(std::string_view token) const;

    std::string_view getToken(size_t idx) const;
    UInt64 getOffsetInFile(size_t idx) const;
    size_t memoryUsageBytes() const;

    /// Returns the raw tokens column. Throws if tokens were bit-packed by optimize.
    ColumnPtr getTokensColumn() const;
    /// Returns the raw offsets column. Throws if offsets were bit-packed by optimize.
    ColumnPtr getOffsetsColumn() const;

    /// Decomposes the tokens column into chars and bit-packed offsets
    /// and bit-packs the offsets in file to reduce memory usage.
    void optimize();

private:
    /// Tokens and offsets in the dictionary file to the beginning of each block.
    /// Stored as raw columns after creation and bit-packed after optimize.
    std::variant<ColumnPtr, BitPackedStringArray> tokens;
    std::variant<ColumnPtr, BitPackedUInt64Array> offsets_in_file;
};

using DictionarySparseIndexPtr = std::shared_ptr<DictionarySparseIndex>;


struct TextIndexHeader
{
    MergeTreeTextIndexSerializationVersion version = MergeTreeTextIndexSerializationVersion::V0_Initial;
    IPostingListCodec::Type codec_type = IPostingListCodec::Type::None;
    /// has_positions and positions_codec are persisted for version >= V2_WithPositions.
    bool has_positions = false;
    UInt8 positions_codec = 0;
    DictionarySparseIndex sparse_index;
};

/// A posting list is serialized either from the builder used while writing an index,
/// or from a plain array of row ids, which merges of text indexes have already materialized.
template <typename T>
concept PostingsContainer = std::same_as<T, PostingListBuilder> || std::same_as<T, std::span<const UInt32>>;

struct TextIndexSerialization
{
    enum class TokensFormat : UInt64
    {
        RawStrings = 0,
        FrontCodedStrings = 1
    };

    /// Serializes the posting list of a single token and returns its metadata.
    /// The row ids must be sorted in ascending order and unique.
    template <PostingsContainer Postings>
    static TokenPostingsInfo serializePostings(
        const Postings & postings,
        MergeTreeIndexWriterStream & postings_stream,
        const MergeTreeIndexTextParams & params,
        PostingsSerialization & postings_serialization);

    static void serializeTokens(const ColumnString & tokens, WriteBuffer & ostr, TokensFormat format);
    static void serializeTokenInfo(WriteBuffer & ostr, const TokenPostingsInfo & token_info);
    /// Reject a token the reader would refuse (throws `TOO_LARGE_STRING_SIZE`); call before copying a token elsewhere.
    static void checkTokenSize(size_t token_size);
    static void serializeHeader(MergeTreeTextIndexSerializationVersion version, const DictionarySparseIndex & sparse_index, IPostingListCodec::Type posting_list_codec_type, bool has_positions, UInt8 positions_codec, WriteBuffer & ostr);

    static TextIndexHeader deserializeHeader(ReadBuffer & istr);
    /// Reads only the version and posting list codec from the start of the header, without the
    /// (potentially large) sparse index. The returned header has an empty `sparse_index`.
    static TextIndexHeader deserializeHeaderPrefix(ReadBuffer & istr);
    /// If skip_postings is true, embedded postings are skipped.
    static TokenPostingsInfo deserializeTokenInfo(ReadBuffer & istr, bool skip_postings = false);
    /// Skips a token info without full deserialization and filling the fields.
    static void skipTokenInfo(ReadBuffer & istr);

    /// Deserializes `TokenPostingsInfo` only for tokens at the given sorted indices,
    /// skipping postings for others. Returns a vector parallel to `matched_indices`.
    static std::vector<TokenPostingsInfoPtr> deserializeTokenInfos(ReadBuffer & istr, size_t num_tokens, const std::vector<size_t> & matched_indices);

    /// Deserializes tokens from a dictionary block.
    /// Returns the tokens column and the tokens format.
    static std::pair<ColumnPtr, UInt64> deserializeTokens(ReadBuffer & istr);

    /// Deserializes a dictionary block into a new DictionaryBlock.
    /// If postings_serialization is null, embedded postings are skipped.
    static DictionaryBlock deserializeDictionaryBlock(ReadBuffer & istr, bool skip_postings = false);
};

using TokenToPostingsMap = absl::flat_hash_map<String, PostingListPtr>;

class TextIndexAnalyzer;

/// Text index granule created on reading of the index.
struct MergeTreeIndexGranuleText final : public IMergeTreeIndexGranule
{
public:
    explicit MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_);
    ~MergeTreeIndexGranuleText() override;

    const MergeTreeIndexTextParams & getParams() const { return params; }

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return is_empty; }
    size_t memoryUsageBytes() const override;

    const TextIndexAnalyzer & getAnalyzer() const { return *analyzer; }

    void setCurrentRange(RowsRange range) { current_range = std::move(range); }
    const std::optional<RowsRange> & getCurrentRange() const { return current_range; }
    const String & getIndexIdForCaches() const { return index_id_for_caches; }
    IPostingListCodec::Type getPostingsCodecType() const { return postings_codec_type; }
    MergeTreeTextIndexSerializationVersion getSerializationVersion() const { return serialization_version; }
    UInt8 getPositionsCodec() const { return positions_codec; }

    static PostingListPtr readPostingsBlock(
        MergeTreeIndexReaderStream & stream,
        MergeTreeIndexDeserializationState & state,
        const TokenPostingsInfo & token_info,
        size_t block_idx,
        PostingsSerialization & postings_serialization,
        const String & index_id_for_caches);

private:
    /// Reads dictionary blocks and analyzes them for tokens.
    void analyzeDictionaryForTokens(const DictionarySparseIndex & sparse_index, MergeTreeIndexReaderStream & dictionary_stream, MergeTreeIndexDeserializationState & state);
    /// Reads dictionary blocks and analyzes them for patterns.
    void analyzeDictionaryForPatterns(const DictionarySparseIndex & sparse_index, MergeTreeIndexReaderStream & dictionary_stream, MergeTreeIndexDeserializationState & state);
    /// Fills tokens and their infos from the cache.
    /// Returns tokens that are not in the cache and need to be read from the dictionary file.
    std::vector<String> fillTokensFromCache(MergeTreeIndexDeserializationState & state);
    std::pair<std::vector<size_t>, NameSet> matchTokens(const ColumnString & all_tokens, std::vector<std::string_view> needed_tokens);

    std::shared_ptr<TextIndexHeader> loadHeader(MergeTreeIndexReaderStream & header_stream, MergeTreeIndexDeserializationState & state);
    void analyzePostings(PostingsSerialization & postings_serialization, MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state);

    bool is_empty = true;
    /// If adding significantly large members here make sure to add them to memoryUsageBytes()
    MergeTreeIndexTextParams params;
    /// Analyzer for the text index. Tracks regular tokens, pattern tokens, and per-query state.
    std::unique_ptr<TextIndexAnalyzer> analyzer;
    /// Current range of rows that is being processed. If set, mayBeTrueOnGranule returns more precise result.
    std::optional<RowsRange> current_range;
    /// Unique identifier for text index in the current data part.
    String index_id_for_caches;
    /// Codec type used to serialize postings in this granule.
    IPostingListCodec::Type postings_codec_type = IPostingListCodec::Type::None;
    /// On-disk serialization version of the text index header.
    MergeTreeTextIndexSerializationVersion serialization_version = MergeTreeTextIndexSerializationVersion::V0_Initial;
    /// Positions on-disk codec persisted in the header.
    UInt8 positions_codec = 0;
};

/// Text index granule created on writing of the index.
/// It differs from MergeTreeIndexGranuleText because it
/// is used only when building the index and stores different data structures.
struct MergeTreeIndexGranuleTextWritable : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleTextWritable(
        MergeTreeIndexTextParams params_,
        IPostingListCodec::Type posting_list_codec_type_,
        TokenToPostingsBuilderMap && tokens_map_,
        std::list<PostingList> && posting_lists_,
        std::unique_ptr<Arena> && arena_,
        std::unique_ptr<TokenToPositionListMap> && position_map_,
        SortedTokens && sorted_tokens_);

    ~MergeTreeIndexGranuleTextWritable() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return sorted_tokens.empty(); }
    size_t memoryUsageBytes() const override;

    /// If adding significantly large members here make sure to add them to memoryUsageBytes()
    MergeTreeIndexTextParams params;
    IPostingListCodec::Type posting_list_codec_type = IPostingListCodec::Type::None;
    TokenToPostingsBuilderMap tokens_map;
    std::list<PostingList> posting_lists;
    std::unique_ptr<Arena> arena;
    /// Owns the PositionListBuilders referenced by sorted_tokens (phrase query support).
    std::unique_ptr<TokenToPositionListMap> position_map;
    /// Sorted view of tokens with their posting/position builders (non-owning; references the fields above).
    SortedTokens sorted_tokens;
    LoggerPtr logger;
};

struct ITokenizer;
using TokenizerPtr = const ITokenizer *;

class MergeTreeIndexTextPostprocessor;
struct MergeTreeIndexTextInlineFilter;

struct MergeTreeIndexTextGranuleBuilder
{
    MergeTreeIndexTextGranuleBuilder(
        MergeTreeIndexTextParams params_,
        TokenizerPtr tokenizer_,
        const IPostingListCodec * posting_list_codec_);

    /// Extracts tokens from the document and adds them to the granule.
    void addDocument(std::string_view document);
    // Adds a document to the granule. The document is inserted directly as a single token.
    void addToken(std::string_view token, UInt32 token_position);

    void incrementCurrentRow();
    void setCurrentRow(size_t row) { current_row = row; }

    std::unique_ptr<MergeTreeIndexGranuleTextWritable> build();
    bool empty() const { return is_empty; }
    void reset();

    void seedDropFilter();

    MergeTreeIndexTextParams params;
    TokenizerPtr tokenizer;
    const IPostingListCodec * posting_list_codec = nullptr;

    bool is_empty = true;
    UInt64 current_row = 0;
    UInt64 num_processed_tokens = 0;
    /// Pointers to posting lists for each token.
    TokenToPostingsBuilderMap tokens_map;
    /// Holder of posting lists. std::list is used to preserve the stability of pointers to posting lists.
    std::list<PostingList> posting_lists;
    /// Keys may be serialized into arena (see ArenaKeyHolder).
    std::unique_ptr<Arena> arena;
    /// Position data for phrase query support.
    /// Only allocated when params.positions is true.
    std::unique_ptr<TokenToPositionListMap> position_map;
    /// IN/NOT IN filter-only postprocessor fast path: `IN` marks dropped tokens in the map on first
    /// insertion, `NOT IN` collects postings only for the pre-seeded keep-set tokens. Non-owning.
    const MergeTreeIndexTextInlineFilter * postprocessor_drop_filter = nullptr;
};

class MergeTreeIndexTextPreprocessor;
using MergeTreeIndexTextPreprocessorPtr = std::shared_ptr<MergeTreeIndexTextPreprocessor>;

class MergeTreeIndexTextPostprocessor;
using MergeTreeIndexTextPostprocessorPtr = std::shared_ptr<MergeTreeIndexTextPostprocessor>;

struct MergeTreeIndexAggregatorText final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorText(
        String index_column_name_,
        MergeTreeIndexTextParams params_,
        TokenizerPtr tokenizer_,
        const IPostingListCodec * posting_list_codec_,
        MergeTreeIndexTextPreprocessorPtr preprocessor_,
        MergeTreeIndexTextPostprocessorPtr postprocessor_);

    ~MergeTreeIndexAggregatorText() override = default;

    bool empty() const override { return granule_builder.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;
    void setCurrentRow(size_t row) { granule_builder.setCurrentRow(row); }
    UInt64 getNumProcessedTokens() const { return granule_builder.num_processed_tokens; }

private:
    /// Iterates over a ColumnArray(String) slice and calls addDocument<tokenize> on each element.
    template <bool tokenize>
    void addDocumentsFromArray(ColumnPtr column, size_t start_row, size_t rows_read);

    String index_column_name;
    MergeTreeIndexTextParams params;
    /// A private clone of the index tokenizer when it is stateful (e.g. the Japanese or sparse-grams
    /// tokenizers), so concurrent aggregators do not share mutable parsing state; null otherwise.
    std::shared_ptr<const ITokenizer> owned_tokenizer;
    TokenizerPtr tokenizer;
    MergeTreeIndexTextGranuleBuilder granule_builder;
    MergeTreeIndexTextPreprocessorPtr preprocessor;
    MergeTreeIndexTextPostprocessorPtr postprocessor;
    /// True when the postprocessor is an IN/NOT IN filter handled by the per-distinct-token drop fast path.
    bool use_postprocessor_drop_fast_path = false;
};

class MergeTreeIndexText final : public IMergeTreeIndex
{
public:
    MergeTreeIndexText(
        StorageMetadataPtr metadata_snapshot_,
        const IndexDescription & index_,
        MergeTreeIndexTextParams params_,
        std::unique_ptr<ITokenizer> tokenizer_,
        std::unique_ptr<IPostingListCodec> posting_list_codec_);

    ~MergeTreeIndexText() override = default;

    MergeTreeIndexTextParams getParams() const { return params; }
    bool isTextIndex() const override { return true; }

    MergeTreeIndexSubstreams getSubstreams() const override;
    using IMergeTreeIndex::getPhysicalFormat;
    MergeTreeIndexFormat getPhysicalFormat(
        const MergeTreeDataPartChecksums & checksums,
        const IDataPartStorage & storage,
        const std::string & relative_path_prefix) const override;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    const IPostingListCodec * getPostingListCodec() const { return posting_list_codec.get(); }
    static DataTypePtr getNestedDataType(const DataTypePtr & data_type);

    MergeTreeIndexTextParams params;
    std::unique_ptr<ITokenizer> tokenizer;
    std::unique_ptr<IPostingListCodec> posting_list_codec;
    MergeTreeIndexTextPreprocessorPtr preprocessor;
    MergeTreeIndexTextPostprocessorPtr postprocessor;
    /// Name of the index expression rewritten as `optimize_empty_string_comparisons` rewrites queries.
    std::optional<String> normalized_index_column_name;
};

}
