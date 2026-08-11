#pragma once

#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/IColumn.h>
#include <Common/BitPackedStringArray.h>
#include <Common/BitPackedUInt64Array.h>
#include <Common/Logger.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/TextIndexPositionData.h>
#include <Formats/MarkInCompressedFile.h>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <base/types.h>

#include <optional>
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

using PostingListCodecPtr = std::unique_ptr<IPostingListCodec>;

struct MergeTreeIndexTextParams
{
    size_t dictionary_block_size = 0;
    size_t dictionary_block_frontcoding_compression = 1;
    size_t posting_list_block_size = 1024 * 1024;
    size_t positions = 0;
    ASTPtr preprocessor;
    ASTPtr postprocessor;
    /// Complete field ownership table supplied by the `field_ids` index argument.
    /// Every active index column has an entry; additional entries are retained tombstones whose
    /// ids cannot be reused. The caller owns the append-only stability contract across metadata
    /// revisions. A null pointer is the legacy untagged single-column dictionary layout; a
    /// non-null pointer selects the tagged layout even when only one column is currently active.
    /// The initial tagged stage rejects `preprocessor` and `postprocessor`; those remain supported
    /// without behavior changes on legacy untagged single-column indexes. Parts do not persist
    /// per-field coverage in this stage, so changing the active fields or their ids is an index
    /// replacement: complete the normal `DROP INDEX` / `ADD INDEX` and materialization lifecycle
    /// before relying on predicates for the changed fields.
    TextIndexFieldIdsMapPtr field_ids;
};

/// Field-tagged dictionary keys use one canonical physical layout:
///
///   [ escaped logical token bytes ][ 0x00 ][ escape flag ][ field id: big-endian `UInt16` ]
///
/// A logical NUL is escaped as 0x00 0xFF. The escape flag is 0x00 for ordinary tokens and 0x01
/// when the token contains an escaped NUL. Both terminators sort before the encoded continuation
/// of a longer token, while the big-endian field ID preserves numeric order. Consequently, sorting
/// physical keys is equivalent to sorting `(logical token, field id)`, so all field variants of one
/// token are contiguous even when another token has it as a prefix. Existing posting and position
/// payload formats remain unchanged.
inline constexpr size_t TEXT_INDEX_FIELD_ID_SIZE = 2;
inline constexpr size_t TEXT_INDEX_TOKEN_TERMINATOR_SIZE = 2;
inline constexpr UInt8 TEXT_INDEX_TOKEN_HAS_ESCAPES = 0x01;
inline constexpr UInt8 TEXT_INDEX_TOKEN_ESCAPED_NUL = 0xFF;
static_assert(sizeof(UInt16) == TEXT_INDEX_FIELD_ID_SIZE);

struct DecodedTextIndexFieldToken
{
    std::string_view token;
    UInt16 field_id;
};

/// Encode the order-preserving token component in place. Keeping this component separate from the
/// field suffix allows a later query stage to construct one contiguous range for all field variants
/// of a token without duplicating the escaping rules.
inline void encodeTextIndexTokenComponent(String & token)
{
    const size_t first_nul = token.find('\0');
    const bool has_escaped_nul = first_nul != String::npos;
    if (has_escaped_nul)
    {
        String escaped;
        escaped.reserve(token.size() + 1 + TEXT_INDEX_TOKEN_TERMINATOR_SIZE + TEXT_INDEX_FIELD_ID_SIZE);

        size_t chunk_begin = 0;
        for (size_t nul = first_nul; nul != String::npos; nul = token.find('\0', chunk_begin))
        {
            escaped.append(token, chunk_begin, nul - chunk_begin);
            escaped.push_back('\0');
            escaped.push_back(static_cast<char>(TEXT_INDEX_TOKEN_ESCAPED_NUL));
            chunk_begin = nul + 1;
        }
        escaped.append(token, chunk_begin);
        token = std::move(escaped);
    }

    token.reserve(token.size() + TEXT_INDEX_TOKEN_TERMINATOR_SIZE + TEXT_INDEX_FIELD_ID_SIZE);
    token.push_back('\0');
    token.push_back(static_cast<char>(has_escaped_nul ? TEXT_INDEX_TOKEN_HAS_ESCAPES : 0x00));
}

inline void appendTextIndexFieldId(String & encoded_token_component, UInt16 field_id)
{
    encoded_token_component.push_back(static_cast<char>((field_id >> 8) & 0xFF));
    encoded_token_component.push_back(static_cast<char>(field_id & 0xFF));
}

/// Encode an owned logical token in place. The query path uses this helper so it cannot diverge
/// from the build-side encoding.
inline void encodeTextIndexFieldToken(String & token, UInt16 field_id)
{
    encodeTextIndexTokenComponent(token);
    appendTextIndexFieldId(token, field_id);
}

/// Encode a non-owning tokenizer result into caller-owned storage for dictionary insertion.
inline void encodeTextIndexFieldToken(std::string_view token, UInt16 field_id, String & encoded)
{
    if (token.empty())
        encoded.clear();
    else
        encoded.assign(token.data(), token.size());
    encodeTextIndexFieldToken(encoded, field_id);
}

/// Decode the escaped logical token and fixed-width field ID. For the common case without escaped
/// NUL bytes, the returned token points directly into `encoded`. Otherwise it points into the
/// caller-owned `token_scratch`, which can be reused after the current view is consumed.
/// `std::nullopt` means the key is not a canonical field-tagged key.
inline std::optional<DecodedTextIndexFieldToken> decodeTextIndexFieldToken(std::string_view encoded, String & token_scratch)
{
    constexpr size_t minimum_size = TEXT_INDEX_TOKEN_TERMINATOR_SIZE + TEXT_INDEX_FIELD_ID_SIZE;
    if (encoded.size() < minimum_size)
        return std::nullopt;

    const size_t field_id_offset = encoded.size() - TEXT_INDEX_FIELD_ID_SIZE;
    const size_t token_end = field_id_offset - TEXT_INDEX_TOKEN_TERMINATOR_SIZE;
    if (encoded[token_end] != '\0')
        return std::nullopt;

    const std::string_view encoded_token = encoded.substr(0, token_end);
    const auto high = static_cast<UInt16>(static_cast<unsigned char>(encoded[field_id_offset]));
    const auto low = static_cast<UInt16>(static_cast<unsigned char>(encoded[field_id_offset + 1]));
    const UInt16 field_id = static_cast<UInt16>((high << 8) | low);

    const auto escape_flag = static_cast<UInt8>(encoded[token_end + 1]);
    if (escape_flag == 0)
    {
        if (encoded_token.find('\0') != std::string_view::npos)
            return std::nullopt;
        return DecodedTextIndexFieldToken{.token = encoded_token, .field_id = field_id};
    }
    if (escape_flag != TEXT_INDEX_TOKEN_HAS_ESCAPES)
        return std::nullopt;

    token_scratch.clear();
    token_scratch.reserve(token_end);
    bool decoded_nul = false;
    for (size_t pos = 0; pos < token_end; ++pos)
    {
        const char byte = encoded[pos];
        if (byte != '\0')
        {
            token_scratch.push_back(byte);
            continue;
        }

        if (++pos >= token_end || static_cast<UInt8>(encoded[pos]) != TEXT_INDEX_TOKEN_ESCAPED_NUL)
            return std::nullopt;
        token_scratch.push_back('\0');
        decoded_nul = true;
    }

    if (!decoded_nul)
        return std::nullopt;

    return DecodedTextIndexFieldToken{.token = token_scratch, .field_id = field_id};
}

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
        SmallContainer small{};
        PostingListWithContext large;
    };

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

struct PostingsSerialization
{
    PostingsSerialization(PostingListCodecPtr posting_list_codec_, MergeTreeIndexVersion serialization_version_);

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

    void serialize(PostingListBuilder & postings, TokenPostingsInfo & info, size_t posting_list_block_size, WriteBuffer & ostr);
    void serialize(const PostingList & postings, TokenPostingsInfo & info, size_t posting_list_block_size, WriteBuffer & ostr);
    void serialize(const roaring::api::roaring_bitmap_t & postings, UInt64 header, WriteBuffer & ostr);
    PostingListPtr deserialize(ReadBuffer & istr, UInt64 header, UInt64 cardinality);
    const IPostingListCodec * getPostingListCodec() const { return posting_list_codec.get(); }

private:
    PostingListCodecPtr posting_list_codec;
    MergeTreeIndexVersion serialization_version;

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
    PostingListPtr embedded_postings;

    /// Position data offset in the .pos file
    UInt64 position_offset = 0;
    /// Number of Roaringish UInt64 entries in position data.
    UInt32 position_cardinality = 0;

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
    enum class Version
    {
        Initial = 0,
        WithCodec = 1,
        WithPositions = 2,
        WithFieldIds = 3,
    };

    MergeTreeIndexVersion version = static_cast<MergeTreeIndexVersion>(Version::Initial);
    IPostingListCodec::Type codec_type = IPostingListCodec::Type::None;
    /// Persisted for version >= `WithPositions`.
    bool has_positions = false;
    /// Persisted for version >= `WithFieldIds`; this distinguishes legacy token keys from
    /// field-tagged keys independently of the current metadata definition.
    bool has_field_ids = false;
    DictionarySparseIndex sparse_index;
};

struct TextIndexSerialization
{
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
    static void serializeHeader(
        const DictionarySparseIndex & sparse_index,
        IPostingListCodec::Type posting_list_codec_type,
        MergeTreeIndexVersion version,
        bool has_positions,
        bool has_field_ids,
        WriteBuffer & ostr);

    static TextIndexHeader deserializeHeader(ReadBuffer & istr);
    /// Reads only the version and posting list codec from the start of the header, without the
    /// (potentially large) sparse index. The returned header has an empty `sparse_index`.
    static TextIndexHeader deserializeHeaderPrefix(ReadBuffer & istr);
    /// If postings_serialization is null, embedded postings are skipped.
    static TokenPostingsInfo deserializeTokenInfo(ReadBuffer & istr, PostingsSerialization * postings_serialization);
    static void skipTokenInfo(ReadBuffer & istr);

    /// Deserializes `TokenPostingsInfo` only for tokens at the given sorted indices,
    /// skipping postings for others. Returns a vector parallel to `matched_indices`.
    static std::vector<TokenPostingsInfoPtr> deserializeTokenInfos(
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
    MergeTreeIndexVersion getSerializationVersion() const { return serialization_version; }

    static PostingListPtr readPostingsBlock(
        MergeTreeIndexReaderStream & stream,
        MergeTreeIndexDeserializationState & state,
        const TokenPostingsInfo & token_info,
        size_t block_idx,
        PostingsSerialization & postings_serialization,
        const String & index_id_for_caches);

private:
    /// Reads dictionary blocks and analyzes them for tokens.
    void analyzeDictionaryForTokens(const DictionarySparseIndex & sparse_index, PostingsSerialization & postings_serialization, MergeTreeIndexReaderStream & dictionary_stream, MergeTreeIndexDeserializationState & state);
    /// Reads dictionary blocks and analyzes them for patterns.
    void analyzeDictionaryForPatterns(const DictionarySparseIndex & sparse_index, PostingsSerialization & postings_serialization, MergeTreeIndexReaderStream & dictionary_stream, MergeTreeIndexDeserializationState & state);
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
    MergeTreeIndexVersion serialization_version = static_cast<MergeTreeIndexVersion>(TextIndexHeader::Version::Initial);
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

    /// Extracts tokens from the document and adds them to the granule. `field_id` is set exactly
    /// for the tagged physical layout; `std::nullopt` is reserved for legacy untagged indexes.
    void addDocument(std::string_view document, std::optional<UInt16> field_id = std::nullopt);
    /// Adds one already-tokenized value, preserving the supplied position for phrase search.
    void addToken(std::string_view token, UInt32 token_position, std::optional<UInt16> field_id = std::nullopt);

    void incrementCurrentRow();
    void setCurrentRow(size_t row) { current_row = row; }

    std::unique_ptr<MergeTreeIndexGranuleTextWritable> build();
    bool empty() const { return is_empty; }
    void reset();

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
    /// Reused scratch storage for one composite key. `ArenaKeyHolder` persists the key bytes into
    /// `arena` during insertion, so dictionary entries never retain a view into this buffer.
    String field_token_scratch;
    /// Position data for phrase query support.
    /// Only allocated when params.positions is true.
    std::unique_ptr<TokenToPositionListMap> position_map;
    /// Fast path for IN/NOT IN filter-only postprocessors: when set, addToken drops a token before inserting it,
    /// so dropped tokens allocate no map entry and build no postings. Non-owning.
    const MergeTreeIndexTextInlineFilter * postprocessor_drop_filter = nullptr;
};

class MergeTreeIndexTextPreprocessor;
using MergeTreeIndexTextPreprocessorPtr = std::shared_ptr<MergeTreeIndexTextPreprocessor>;

class MergeTreeIndexTextPostprocessor;
using MergeTreeIndexTextPostprocessorPtr = std::shared_ptr<MergeTreeIndexTextPostprocessor>;

struct MergeTreeIndexAggregatorText final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorText(
        Names index_column_names_,
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
    /// Legacy untagged single-column path.
    template <bool tokenize>
    void addDocumentsFromArray(ColumnPtr column, size_t start_row, size_t rows_read);

    /// One or more active index columns in declaration order. `params.field_ids != nullptr` is the
    /// sole runtime discriminator for the physical key layout, including tagged single-column indexes.
    Names index_column_names;
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
    MergeTreeIndexFormat getDeserializedFormat(const IMergeTreeDataPart & part, const std::string & relative_path_prefix) const override;

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
};

}
