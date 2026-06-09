#pragma once

#include <Storages/MergeTree/IPostingListCodec.h>
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

#include <array>
#include <memory>
#include <new>
#include <span>
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
    ASTPtr preprocessor;
};

using PostingList = roaring::Roaring;
using PostingListPtr = std::shared_ptr<PostingList>;

/// A struct for building a posting list during the index build.
///
/// The first `inline_capacity` row ids are stored inline in a fixed-size array — these are tokens
/// rare enough to be serialized as raw VarUInts (or embedded into the dictionary block), so they
/// never need a Roaring bitmap or a codec. Once the inline buffer overflows, the builder "promotes"
/// to a streaming IPostingListAccumulator and forwards all subsequent row ids to it, so larger
/// posting lists are encoded directly into their codec's on-disk form during the build.
///
/// The accumulator is owned externally (in the granule builder) so that the builder stays
/// trivially relocatable and can live by value inside the StringHashMap.
///
/// Row ids are added in non-descending order; consecutive duplicates are skipped.
struct PostingListBuilder
{
    /// Number of row ids stored inline before promoting to an accumulator.
    /// Sized to MAX_CARDINALITY_FOR_RAW_POSTINGS so the whole raw/embedded range stays heap-free.
    static constexpr size_t inline_capacity = 12;
    using SmallContainer = std::array<UInt32, inline_capacity>;

    /// Holds accumulators for promoted tokens. A vector is enough: the builders store a raw pointer
    /// to the pointee (not into this buffer), and the unique_ptr keeps that pointee stable across
    /// vector reallocations — which only move the 8-byte unique_ptrs, not the accumulator objects.
    using AccumulatorHolder = std::vector<std::unique_ptr<IPostingListAccumulator>>;

    /// Adds a row id. On overflow of the inline buffer, an accumulator is created via `codec`
    /// (stored in `holder`) and the buffered values are replayed into it.
    ///
    /// Templated on the concrete accumulator type so that the per-row hot path can call
    /// `Accumulator::insert` directly (no virtual dispatch); the caller picks `Accumulator`
    /// once via `dispatchByPostingCodec`. `Accumulator` must match `codec`'s actual type.
    template <typename Accumulator>
    void add(UInt32 value, const IPostingListCodec & codec, size_t posting_list_block_size, AccumulatorHolder & holder)
    {
        /// Values are added in non-descending order; skip consecutive duplicates.
        if (small_size != 0)
        {
            chassert(value >= last_value);
            if (value == last_value)
                return;
        }

        last_value = value;

        if (promoted)
        {
            static_cast<Accumulator *>(promoted_state.accumulator)->insert(value, posting_list_block_size, promoted_state.bulk_context);
            return;
        }

        if (small_size < inline_capacity)
        {
            small[small_size++] = value;
            return;
        }

        /// The inline buffer is full: promote to a streaming accumulator and replay the buffered
        /// values. The promoted state (accumulator pointer + bulk context) shares storage with `small`
        /// (union), so snapshot the buffer first.
        const SmallContainer buffered = small;
        holder.push_back(codec.createAccumulator());
        auto * acc = static_cast<Accumulator *>(holder.back().get());

        /// Start the lifetime of the `promoted_state` union member (small is now dead). Placement-new
        /// because `roaring::BulkContext` is not trivially constructible. The context starts empty (no
        /// cached container yet) and lives here so it stays cache-warm next to the token's hash-map
        /// entry; it is passed by reference into every `insert`.
        ::new (&promoted_state) PromotedState{acc, roaring::BulkContext{}};
        promoted = true;

        for (UInt32 buffered_value : buffered)
            acc->insert(buffered_value, posting_list_block_size, promoted_state.bulk_context);

        acc->insert(value, posting_list_block_size, promoted_state.bulk_context);
    }

    size_t size() const { return promoted ? promoted_state.accumulator->cardinality() : small_size; }
    bool isEmpty() const { return !promoted && small_size == 0; }
    bool hasAccumulator() const { return promoted; }
    IPostingListAccumulator & getAccumulator() const { return *promoted_state.accumulator; }

    /// Inline row ids. Valid only while not promoted (i.e. size() <= inline_capacity).
    std::span<const UInt32> getSmallValues() const { return {small.data(), small_size}; }

    /// Minimum / maximum row id. Only used for the non-promoted (raw) path.
    UInt32 minimum() const { return small[0]; }
    UInt32 maximum() const { return small[small_size - 1]; }

    /// The (externally owned) accumulator pointer paired with the roaring bulk-insert context.
    /// Keeping `bulk_context` here — rather than inside the separately allocated accumulator — means
    /// the per-row `roaring_bitmap_add_bulk` reads `bulk_context.container` from the same cache line as
    /// the token's hash-map entry (already warmed by the lookup), instead of chasing a pointer into a
    /// cold accumulator object on every row. It costs no extra builder bytes: it overlaps the dead
    /// inline buffer (see the union below). `bitpacking` ignores the context.
    struct PromotedState
    {
        IPostingListAccumulator * accumulator;
        roaring::BulkContext bulk_context;
    };

    /// Before promotion the inline buffer holds the row ids; after promotion that storage is reused
    /// for the promoted state, discriminated by `promoted`.
    union
    {
        SmallContainer small{};
        PromotedState promoted_state;
    };
    UInt32 last_value = 0;
    UInt8 small_size = 0;
    bool promoted = false;
};

using TokenToPostingsBuilderMap = StringHashMap<PostingListBuilder>;
using SortedTokensAndPostings = std::vector<std::pair<std::string_view, PostingListBuilder *>>;
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
    };

    PostingListPtr deserialize(ReadBuffer & istr, UInt64 header, UInt64 cardinality);
    const IPostingListCodec * getPostingListCodec() const { return posting_list_codec.get(); }

private:
    PostingListCodecPtr posting_list_codec;
    MergeTreeIndexVersion serialization_version;

    /// Reusable buffer to avoid repeated heap allocations when deserializing
    /// small posting lists stored as raw VarUInts.
    std::vector<UInt32> raw_postings_buffer;
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

    /// Returns indexes of posting list blocks to read for the given range of rows.
    std::vector<size_t> getBlocksToRead(const RowsRange & range) const;
    size_t bytesAllocated() const;
};

using TokenPostingsInfoPtr = std::shared_ptr<TokenPostingsInfo>;
using TokenToPostingsInfosMap = absl::flat_hash_map<String, TokenPostingsInfoPtr>;

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


struct TextIndexHeader
{
    enum class Version
    {
        Initial = 0,
        WithCodec = 1,
    };

    MergeTreeIndexVersion version = static_cast<MergeTreeIndexVersion>(Version::Initial);
    IPostingListCodec::Type codec_type = IPostingListCodec::Type::None;
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
        MergeTreeIndexWriterStream & postings_stream);

    static void serializeTokens(const ColumnString & tokens, WriteBuffer & ostr, TokensFormat format);
    static void serializeTokenInfo(WriteBuffer & ostr, const TokenPostingsInfo & token_info);
    /// Writes row ids as raw VarUInts (used for embedded and small raw posting lists).
    static void serializeRawPostings(std::span<const UInt32> row_ids, WriteBuffer & ostr);
    static void serializeHeader(const DictionarySparseIndex & sparse_index, IPostingListCodec::Type posting_list_codec_type, WriteBuffer & ostr);

    static TextIndexHeader deserializeHeader(ReadBuffer & istr);
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

    bool hasAnyQueryTokens(const TextSearchQuery & query) const;
    bool hasAnyQueryPatterns(const TextSearchQuery & query) const;

    bool hasAllQueryTokens(const TextSearchQuery & query) const;
    bool hasAllQueryTokensOrEmpty(const TextSearchQuery & query) const;

    const TextIndexAnalyzer & getAnalyzer() const { return *analyzer; }

    void setCurrentRange(RowsRange range) { current_range = std::move(range); }
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
    bool hasAnyTokensImpl(const TextSearchQuery & query) const;

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
        SortedTokensAndPostings && tokens_and_postings_,
        TokenToPostingsBuilderMap && tokens_map_,
        PostingListBuilder::AccumulatorHolder && accumulators_,
        std::unique_ptr<Arena> && arena_);

    ~MergeTreeIndexGranuleTextWritable() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return tokens_and_postings.empty(); }
    size_t memoryUsageBytes() const override;

    /// If adding significantly large members here make sure to add them to memoryUsageBytes()
    MergeTreeIndexTextParams params;
    IPostingListCodec::Type posting_list_codec_type = IPostingListCodec::Type::None;
    /// Pointers to tokens and posting lists in the granule.
    SortedTokensAndPostings tokens_and_postings;
    /// tokens_and_postings has references to data held in the fields below.
    TokenToPostingsBuilderMap tokens_map;
    /// Accumulators for promoted (large) tokens. Referenced by raw pointers in the builders.
    PostingListBuilder::AccumulatorHolder accumulators;
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
        const IPostingListCodec * posting_list_codec_);

    /// Extracts tokens from the document and adds them to the granule.
    void addDocument(std::string_view document);
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
    /// Posting list builder for each token.
    TokenToPostingsBuilderMap tokens_map;
    /// Holder of accumulators for promoted tokens (see PostingListBuilder::AccumulatorHolder).
    PostingListBuilder::AccumulatorHolder accumulators;
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
        const IPostingListCodec * posting_list_codec_,
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
    const IPostingListCodec * posting_list_codec = nullptr;
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

    const IPostingListCodec * getPostingListCodec() const { return posting_list_codec.get(); }
    static DataTypePtr getNestedDataType(const DataTypePtr & data_type);

    MergeTreeIndexTextParams params;
    std::unique_ptr<ITokenizer> tokenizer;
    std::unique_ptr<IPostingListCodec> posting_list_codec;
    MergeTreeIndexTextPreprocessorPtr preprocessor;
};

}
