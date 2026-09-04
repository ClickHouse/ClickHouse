#pragma once

#include <Core/SettingsEnums.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/BitPackedStringArray.h>
#include <Common/BitPackedUInt64Array.h>
#include <Common/Logger.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/TextIndexPositionData.h>
#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <Formats/MarkInCompressedFile.h>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <base/defines.h>
#include <base/types.h>

#include <algorithm>

#include <optional>
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
  * 3. Posting lists are encoded with the configured posting list codec ('none' (raw Roaring Bitmaps), 'bitpacking').
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
  * Otherwise, the posting list is split into segments of `posting_list_block_size` row ids and
  * serialized via the configured `IPostingListCodec`.
  *  - the `none` codec writes each segment as a portable Roaring Bitmap with a leading VarUInt size
  *  - the `bitpacking` codec uses a compact bit-packed format with its own segment header.
  */

using PostingListCodecPtr = std::unique_ptr<IPostingListCodec>;

struct MergeTreeIndexTextParams
{
    size_t dictionary_block_size = 0;
    size_t dictionary_block_frontcoding_compression = 1;
    size_t posting_list_block_size = 1024 * 1024;
    bool enable_positions = false;
    bool enable_scoring = false;
    UInt8 positions_codec = static_cast<UInt8>(TextIndexPositionCodec::Encoding::BlockedPfor);
    ASTPtr preprocessor;
    ASTPtr postprocessor;
    MergeTreeTextIndexSerializationVersion serialization_version = MergeTreeTextIndexSerializationVersion::V0_Initial;
};

using PostingList = roaring::Roaring;
using PostingListPtr = std::shared_ptr<PostingList>;

/// Flat postings of one posting-list block decoded for BM25 scoring.
/// Sorted row ids and per-row term frequencies (parallel arrays).
struct ScoringPostings
{
    PaddedPODArray<UInt32> row_ids;
    PaddedPODArray<UInt32> term_frequencies;
    UInt8 max_tf_minus_one = 0;

    void calculateMaxTermFrequency()
    {
        const UInt32 max_tf = term_frequencies.empty() ? 1u : *std::ranges::max_element(term_frequencies);
        max_tf_minus_one = static_cast<UInt8>(std::min<UInt32>(255u, max_tf - 1u));
    }
};

/// The decoded flat postings are immutable and shared between the postings cache,
/// the granules and the scoring cursors.
using ScoringPostingsPtr = std::shared_ptr<const ScoringPostings>;

/// Builds per-row `(tf - 1)` of one token during the index build (BM25 scoring), parallel
/// to the posting-list builder's buffered row ids. Created lazily on the token's first in-row repeat.
class TermFrequenciesBuilder
{
public:
    /// Created on the first in-row repeat: all `num_buffered_rows` rows so far
    /// have `tf == 1`, except the last one, which just repeated (`tf == 2`).
    explicit TermFrequenciesBuilder(size_t num_buffered_rows)
    {
        chassert(num_buffered_rows != 0);
        tf_minus_one.resize_fill(num_buffered_rows, 0u);
        tf_minus_one.back() = 1;
    }

    ALWAYS_INLINE void addRowRepeat()
    {
        chassert(!tf_minus_one.empty());
        ++tf_minus_one.back();
    }

    ALWAYS_INLINE void addNewRow() { tf_minus_one.push_back(0); }
    std::span<const UInt32> getTfMinusOne() const { return {tf_minus_one.data(), tf_minus_one.size()}; }

private:
    PODArray<UInt32, 64> tf_minus_one;
};

/// Everything a `PostingListBuilder` needs to add row ids and flush full blocks into the encoder.
struct PostingListBuildContext
{
    const IPostingListCodec & codec;
    size_t segment_size;
    bool enable_positions;
    bool enable_scoring;
    const PaddedPODArray<UInt8> * doc_lengths;
    UInt32 doc_lengths_first_row_id = 0;
};

/// Builds one token's posting list during the index build.
/// Up to inline_capacity row ids live inline (no heap allocation for the many rare tokens).
/// Frequent tokens spill to `Large`, whose raw values flush to an encoder every `append_granularity` row ids.
/// `Large` is also the extension point for optional per-token payloads that cannot live inline:
/// positions for phrase search, term frequencies for BM25 scoring.
struct PostingListBuilder
{
public:
    static constexpr size_t inline_capacity = 11;

    struct Inline
    {
        std::array<UInt32, inline_capacity> values;
        UInt8 size;
    };

    struct Large
    {
        /// Spills a token from the inline storage to the heap.
        Large(std::array<UInt32, inline_capacity> values_, UInt8 inline_size_);
        Large(std::array<UInt32, inline_capacity> values_, UInt8 inline_size_, UInt32 added_value_);

        /// Starts a token on the heap from its first occurrence, with position tracking enabled.
        Large(UInt32 first_value, UInt32 first_position);

        /// Records an in-row repeat of `row_id` for BM25 scoring.
        void addRowRepeat(UInt32 row_id);

        /// Raw row ids of the current (possibly incomplete) segment.
        PODArray<UInt32, 64> values;
        /// Full segments encoded by the codec. Created lazily on the first flush.
        std::unique_ptr<IPostingListEncoder> encoder;
        /// Positions of the token for phrase search. Null unless positions are enabled.
        std::unique_ptr<PositionListBuilder> positions;
        /// Per-row term frequencies for BM25 scoring. Created lazily on the token's first in-row repeat.
        std::unique_ptr<TermFrequenciesBuilder> term_frequencies;

        /// Flushes all buffered row ids into the encoder and clears the buffer.
        /// The caller should control the flush size.
        void flush(const PostingListBuildContext & context);
    };

    /// A filtered entry holds no postings and is skipped by `build`.
    /// See the IN/NOT IN fast path in the postprocessor.
    struct Filtered
    {
    };

    PostingListBuilder() = default;

    /// Constructs the builder directly in the `Filtered` state.
    explicit PostingListBuilder(Filtered) : state(Filtered{}) {}

    /// The builder is constructed with the first value of a token.
    /// With positions enabled it starts in the `Large` state right away.
    PostingListBuilder(UInt32 first_value, UInt32 first_position, const PostingListBuildContext & context);

    /// Adds a value to the inline array or to the large (heap) buffer.
    /// Flushes full blocks to the encoder as the buffer fills.
    /// When positions are enabled, records the position of the token within the row.
    void add(UInt32 value, UInt32 position, const PostingListBuildContext & context);

    bool hasLarge() const { return std::holds_alternative<Large>(state); }
    bool hasInline() const { return std::holds_alternative<Inline>(state); }
    bool isFiltered() const { return std::holds_alternative<Filtered>(state); }

    Large & getLarge() { return std::get<Large>(state); }
    Inline & getInline() { return std::get<Inline>(state); }
    PositionListBuilder * getPositions();

private:
    std::variant<Inline, Large, Filtered> state;
};

using TokenToPostingsBuilderMap = StringHashMap<PostingListBuilder>;

struct SortedToken
{
    std::string_view token;
    PostingListBuilder * postings = nullptr;
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
static_assert(PostingListBuilder::inline_capacity <= MAX_CARDINALITY_FOR_RAW_POSTINGS, "inline_capacity must not exceed MAX_CARDINALITY_FOR_RAW_POSTINGS");

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
        /// If set, the posting list carries per-document term frequencies for BM25 scoring.
        HasTermFrequencies = 1ULL << 6,
    };

    /// Deserializes a single posting list block and adds the decoded row ids to `postings`.
    /// If `term_frequencies` is not null, it is filled with one term frequency per decoded row id,
    void deserializeToBitmap(ReadBuffer & istr, UInt64 header, UInt64 cardinality, PostingList & postings, PaddedPODArray<UInt32> * term_frequencies);

    /// The same, but writes the decoded row ids to a plain array.
    /// The previous contents of the output arrays are discarded.
    void deserializeToArray(ReadBuffer & istr, UInt64 header, UInt64 cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> * term_frequencies);

    const IPostingListCodec * getPostingListCodec() const { return posting_list_codec.get(); }
    MergeTreeTextIndexSerializationVersion getSerializationVersion() const { return serialization_version; }

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
    /// On the read path holds the exact per-row `tf`; on the merge write path holds
    /// the per-row `(tf - 1)` as serialized on disk. Empty without HasTermFrequencies.
    absl::InlinedVector<UInt32, MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS> embedded_term_frequencies;

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


/// Per-part statistics required for BM25 scoring.
struct ScoringStats
{
    /// Rows per `.dl` segment when writing a `V3_WithScoring` index.
    static constexpr UInt64 DOC_LENGTHS_SEGMENT_SIZE = 128 * 1024;

    /// Total number of documents in the data part.
    UInt64 num_docs = 0;
    /// Total sum of document lengths in the data part.
    UInt64 sum_doc_length = 0;
    /// Segment size and offsets of the document lengths in the `.dl` substream.
    UInt64 doc_lengths_segment_size = 0;
    VectorWithMemoryTracking<UInt64> doc_lengths_segment_offsets;

    bool hasSegmentedDocLengths() const { return !doc_lengths_segment_offsets.empty(); }
};

struct TextIndexHeader
{
    MergeTreeTextIndexSerializationVersion version = MergeTreeTextIndexSerializationVersion::V0_Initial;

    /// Persisted for version >= V1_WithCodec.
    IPostingListCodec::Type codec_type = IPostingListCodec::Type::None;
    /// has_positions and positions_codec are persisted for version >= V2_WithPositions.
    bool has_positions = false;
    UInt8 positions_codec = 0;
    /// Persisted for version >= V3_WithScoring.
    bool has_scoring = false;

    DictionarySparseIndex sparse_index;
    ScoringStats scoring_stats;
};

struct TextIndexSerialization
{
    enum class TokensFormat : UInt64
    {
        RawStrings = 0,
        FrontCodedStrings = 1
    };

    /// Serializes a token's posting list from the build-time `PostingListBuilder` and writes its `TokenPostingsInfo`
    /// into the dictionary stream, along with optional BM25 scoring payloads and optional positions.
    static void serializePostingsAndTokenInfo(
        PostingListBuilder && postings,
        const PostingListBuildContext & context,
        MergeTreeIndexWriterStream & dictionary_stream,
        MergeTreeIndexWriterStream & postings_stream,
        MergeTreeIndexWriterStream * positions_stream);

    static void serializeTokens(const ColumnString & tokens, WriteBuffer & ostr, TokensFormat format);
    static void serializeTokenInfo(WriteBuffer & ostr, const TokenPostingsInfo & token_info);
    static void serializeRawPostings(std::span<const UInt32> row_ids, std::span<const UInt32> tf_minus_one, WriteBuffer & ostr);
    static void serializeHeader(const TextIndexHeader & header, WriteBuffer & ostr);

    /// Reject a token the reader would refuse (throws `TOO_LARGE_STRING_SIZE`); call before copying a token elsewhere.
    static void checkTokenSize(size_t token_size);
    static TextIndexHeader deserializeHeader(ReadBuffer & istr);

    /// Reads the version, posting list codec, feature flags and the BM25 corpus stats (`num_docs`, `sum_doc_length`).
    /// Skips the doc-lengths segment offsets and the sparse index, which stay empty in the result.
    static TextIndexHeader deserializeHeaderPrefix(ReadBuffer & istr);

    /// If `with_postings` is false, embedded postings (and their inline term frequencies) are skipped.
    static TokenPostingsInfo deserializeTokenInfo(ReadBuffer & istr, bool with_postings);

    /// Skips a token info without full deserialization and filling the fields.
    static void skipTokenInfo(ReadBuffer & istr);

    /// Deserializes `TokenPostingsInfo` only for tokens at the given sorted indices, skipping postings for others.
    static std::vector<TokenPostingsInfoPtr> deserializeTokenInfos(ReadBuffer & istr, size_t num_tokens, const std::vector<size_t> & matched_indices);

    /// Deserializes tokens from a dictionary block.
    /// Returns the tokens column and the tokens format.
    static std::pair<ColumnPtr, UInt64> deserializeTokens(ReadBuffer & istr);

    /// Deserializes a dictionary block into a new DictionaryBlock.
    /// If `with_postings` is false, embedded postings (and their inline term frequencies) are skipped.
    static DictionaryBlock deserializeDictionaryBlock(ReadBuffer & istr, bool with_postings);
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

    const ScoringStats & getScoringStats() const { return scoring_stats; }
    bool isScoringEnabled() const { return scoring_enabled; }

    struct PostingsBlock
    {
        PostingListPtr postings;
        /// Filled only when the block is read for scoring.
        ScoringPostingsPtr scoring;
    };

    /// Reads a single posting-list block, through the postings cache.
    /// If `with_scoring` is true, the row ids are also returned as a flat sorted array, together with
    /// the per-row term frequencies (when the posting list stores them) deserialized in the same pass.
    static PostingsBlock readPostingsBlock(
        MergeTreeIndexReaderStream & stream,
        MergeTreeIndexDeserializationState & state,
        const TokenPostingsInfo & token_info,
        size_t block_idx,
        PostingsSerialization & postings_serialization,
        const String & index_id_for_caches,
        bool with_scoring);

    /// Flat postings of the posting-list block at `offset_in_file`, decoded during the granule
    /// analysis. Returns null if the block was not read for scoring.
    ScoringPostingsPtr getScoringPostings(UInt64 offset_in_file) const;

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
    /// Per-part statistics for BM25 scoring, read from the text index header.
    ScoringStats scoring_stats;
    /// Flat postings of the single-block tokens decoded for BM25 scoring during the granule
    /// analysis, keyed by the block's offset in the postings file.
    absl::flat_hash_map<UInt64, ScoringPostingsPtr> scoring_postings_by_offset;
    /// Whether the query computes `_bm25_score` with this index.
    bool scoring_enabled = false;
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
        std::unique_ptr<Arena> && arena_,
        SortedTokens && sorted_tokens_,
        PaddedPODArray<UInt8> && doc_lengths_,
        UInt64 num_docs_,
        UInt64 sum_doc_length_);

    ~MergeTreeIndexGranuleTextWritable() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return sorted_tokens.empty(); }
    size_t memoryUsageBytes() const override;

    MergeTreeIndexTextParams params;
    IPostingListCodec::Type posting_list_codec_type = IPostingListCodec::Type::None;
    TokenToPostingsBuilderMap tokens_map;
    std::unique_ptr<Arena> arena;
    /// Sorted view of tokens with their posting/position builders (non-owning; references the fields above).
    SortedTokens sorted_tokens;
    LoggerPtr logger;

    /// BM25 scoring state, carried from the granule builder.
    PaddedPODArray<UInt8> doc_lengths;
    UInt64 num_docs = 0;
    UInt64 sum_doc_length = 0;
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

    /// The context for `addDocument`/`addToken`; created once per batch of added documents.
    PostingListBuildContext buildContext() const;
    /// Extracts tokens from the document and adds them to the granule.
    void addDocument(std::string_view document, const PostingListBuildContext & context);
    // Adds a document to the granule. The document is inserted directly as a single token.
    void addToken(std::string_view token, UInt32 token_position, const PostingListBuildContext & context);

    void incrementCurrentRow();

    /// Continues the row numbering of an empty builder from `row`, so that the row ids of the next temporary
    /// segment of the index materialization stay absolute within the part. Its `doc_lengths` start at `row`.
    void setCurrentRow(size_t row)
    {
        chassert(is_empty && doc_lengths.empty());
        current_row = row;
    }

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
    UInt64 tokens_in_current_row = 0;

    /// Posting list builders for each token.
    TokenToPostingsBuilderMap tokens_map;
    /// Keys may be serialized into arena (see ArenaKeyHolder).
    std::unique_ptr<Arena> arena;
    /// IN/NOT IN filter-only postprocessor fast path: `IN` marks dropped tokens in the map on first
    /// insertion, `NOT IN` collects postings only for the pre-seeded keep-set tokens. Non-owning.
    const MergeTreeIndexTextInlineFilter * postprocessor_drop_filter = nullptr;
    /// Per-row document length quantised to a single `SmallFloat` byte, accumulated when scoring is on.
    PaddedPODArray<UInt8> doc_lengths;
    /// Sum of per-row document lengths, used as a per-part statistic for BM25 scoring.
    UInt64 sum_doc_length = 0;
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
    void addDocumentsFromArray(ColumnPtr column, size_t start_row, size_t rows_read, const PostingListBuildContext & context);

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
    using IMergeTreeIndex::createIndexCondition;

    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context, bool scoring_enabled) const;

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
