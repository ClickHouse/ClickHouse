#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Common/SipHash.h>

#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/// This header is included by widely-used translation units (e.g. `RPNBuilder`), so only
/// forward-declare `roaring::Roaring` here; it is used solely as a by-value return type.
namespace roaring
{
class Roaring;
}

namespace DB
{

struct ITokenizer;
class MergeTreeIndexTextPreprocessor;
using MergeTreeIndexTextPreprocessorPtr = std::shared_ptr<MergeTreeIndexTextPreprocessor>;

struct MergeTreeIndexBloomSlicedParams
{
    size_t bits = 8192;
    size_t hashes = 4;
    size_t min_hashes = 1;
    bool bits_explicit = false;
    bool hashes_explicit = false;
    bool infer_from_false_positive_rate = true;
    double false_positive_rate = 0.05;
    size_t rows_per_signature = 16;
    size_t index_granularity_rows = 8192;
    ASTPtr preprocessor;
    /// True when the preprocessor is present and is not a pure case fold of the index column.
    /// Such preprocessors can destroy tokens of the raw column, so the index additionally stores
    /// one tombstone Bloom filter of lost raw tokens per chunk (see `BloomSlicedChunkMetadata`).
    /// The flag is derived from the index declaration on both the write and the read side, so it
    /// also decides whether the per-chunk tombstone section is present in the serialized metadata.
    bool has_lossy_preprocessor = false;
};

struct BloomSlicedIndexRowRange
{
    size_t begin = 0;
    size_t end = 0;
};

/// One required raw token of a predicate under a lossy preprocessor, together with the tokens the
/// main slices are probed with. `probe_tokens` is the needle pipeline applied to the raw token:
/// `tokenize(processConstant(raw_token))`, computed by the same code path the build side uses when
/// it decides whether a raw token is lost (see the tombstone comment in `predicateFromFunctionNode`).
struct BloomSlicedTokenGroup
{
    String raw_token;
    std::vector<String> probe_tokens;
};

struct BloomSlicedTokenPredicate
{
    String function_name;
    /// Required tokens in the stored (preprocessed) namespace; used when the preprocessor is
    /// absent or a pure case fold. Empty when `token_groups` is used.
    std::vector<String> tokens;
    /// Required raw tokens with their probe mappings; used under a lossy preprocessor together
    /// with the per-chunk tombstone Bloom filters. Empty when `tokens` is used.
    std::vector<BloomSlicedTokenGroup> token_groups;

    SipHash getHash() const;
};

enum class BloomSlicedBitmapCodec : UInt8
{
    Empty = 0,
    Dense = 1,
    RawZstd = 2,
};

struct BloomSlicedBitmapMetadata
{
    UInt64 offset = 0;
    UInt64 compressed_size = 0;
    UInt64 cardinality = 0;
    BloomSlicedBitmapCodec codec = BloomSlicedBitmapCodec::Empty;
};

struct BloomSlicedChunkMetadata
{
    UInt64 first_group = 0;
    UInt64 group_count = 0;
    UInt64 first_row = 0;
    UInt64 row_count = 0;
    std::vector<BloomSlicedBitmapMetadata> bitmap_metadata;
    std::vector<std::vector<char>> raw_bitmaps;
    /// Variable-hash metadata is chunk-local. The key is the first half of the token Bloom hash pair;
    /// the value is the minimum hash count among all tokens in the chunk with that key. Storing the
    /// minimum keeps hash1 collisions safe: a colliding query token may check fewer slices, but never
    /// more than were set for a real matching token.
    std::unordered_map<UInt64, UInt64> token_hash_counts;
    /// Tombstone Bloom filter over the distinct raw tokens of the chunk that the lossy preprocessor
    /// destroyed (raw token `t` is lost iff `tokenize(processConstant(t))` is not fully contained in
    /// the stored token set of some row of the chunk). Probed with raw needle tokens; a hit makes the
    /// whole chunk fail open for that token. Present only when the index has a lossy preprocessor;
    /// empty when the chunk has no lost tokens.
    UInt64 tombstone_bits = 0;
    UInt64 tombstone_hashes = 0;
    UInt64 tombstone_token_count = 0;
    std::vector<char> tombstone_bloom;
    bool loaded = false;
};

struct BloomSlicedHashPairKey
{
    UInt64 hash1 = 0;
    UInt64 hash2 = 0;

    bool operator==(const BloomSlicedHashPairKey & other) const { return hash1 == other.hash1 && hash2 == other.hash2; }
};

struct BloomSlicedHashPairKeyHash
{
    size_t operator()(const BloomSlicedHashPairKey & key) const;
};

struct BloomSlicedVariableTokenGroups
{
    UInt32 last_local_group = std::numeric_limits<UInt32>::max();
    std::vector<UInt32> local_groups;
};

struct MergeTreeIndexGranuleBloomSliced final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleBloomSliced(MergeTreeIndexBloomSlicedParams params_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return row_count == 0; }
    size_t memoryUsageBytes() const override;

    void setCurrentRange(BloomSlicedIndexRowRange range) { current_range = range; }
    const std::optional<BloomSlicedIndexRowRange> & getCurrentRange() const { return current_range; }

    roaring::Roaring allRowsBitmap() const;
    roaring::Roaring bitmapForPredicate(const BloomSlicedTokenPredicate & predicate) const;
    roaring::Roaring bitmapForTokens(const std::vector<String> & tokens) const;
    roaring::Roaring bitmapForTokenGroups(const std::vector<BloomSlicedTokenGroup> & token_groups) const;
    UInt64 groupCount() const;
    UInt64 groupsPerChunk() const;

    MergeTreeIndexBloomSlicedParams params;
    UInt64 row_count = 0;
    std::vector<BloomSlicedChunkMetadata> chunks;
    std::optional<BloomSlicedIndexRowRange> current_range;
};

struct MergeTreeIndexAggregatorBloomSliced final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorBloomSliced(
        MergeTreeIndexBloomSlicedParams params_, const ITokenizer * tokenizer_, MergeTreeIndexTextPreprocessorPtr preprocessor_);

    bool empty() const override { return row_count == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    void addFixedHashTokenToGroup(const char * data, size_t length, UInt64 group_id);
    void addVariableHashTokenToGroup(const char * data, size_t length, UInt64 group_id);
    void ensureFixedHashChunkForGroup(UInt64 group_id);
    void ensureVariableHashChunkForGroup(UInt64 group_id);
    void flushFixedHashChunk(UInt64 group_count);
    void flushVariableHashChunk(UInt64 group_count);
    void inferParamsFromCurrentChunk(UInt64 group_count);
    void collectTombstoneTokensForRow(const char * raw_data, size_t raw_size, const std::unordered_set<std::string_view> & stored_tokens);
    void finishTombstoneBloomForChunk(BloomSlicedChunkMetadata & chunk);

    MergeTreeIndexBloomSlicedParams params;
    const ITokenizer * tokenizer;
    MergeTreeIndexTextPreprocessorPtr preprocessor;
    /// Whether tokens are buffered per group for variable-hash processing. Frozen at construction
    /// (before any row is consumed) so that a parameter inference at the first chunk flush cannot
    /// flip the build path mid-part; see the constructor for details.
    bool variable_hash_buffering = false;
    UInt64 row_count = 0;
    std::vector<BloomSlicedChunkMetadata> chunked_hash_chunks;
    std::vector<std::vector<char>> chunk_raw_bitmaps;
    std::vector<UInt64> chunk_cardinalities;
    std::unordered_map<BloomSlicedHashPairKey, BloomSlicedVariableTokenGroups, BloomSlicedHashPairKeyHash> variable_hash_chunk_tokens;
    /// Tombstone state, used only when `params.has_lossy_preprocessor`: distinct lost raw tokens
    /// of the current chunk (deduplicated by their Bloom hash pair - the only form the tombstone
    /// Bloom filter consumes) and a per-chunk memoization of the raw-token probe mapping
    /// `tokenize(processConstant(token))`, which is row-independent.
    std::unordered_set<BloomSlicedHashPairKey, BloomSlicedHashPairKeyHash> chunk_tombstone_hashes;
    std::unordered_map<String, std::vector<String>> chunk_probe_tokens_cache;
    std::unordered_set<std::string_view> row_stored_tokens;
    UInt64 chunk_first_group = 0;
    bool has_chunk = false;
};

class MergeTreeIndexConditionBloomSliced final : public IMergeTreeIndexCondition
{
public:
    struct RPNElement
    {
        enum Function
        {
            FUNCTION_TOKEN_PREDICATE,
            FUNCTION_UNKNOWN,
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        Function function = FUNCTION_UNKNOWN;
        std::optional<BloomSlicedTokenPredicate> predicate;
    };

    MergeTreeIndexConditionBloomSliced(
        const ActionsDAG::Node * predicate,
        ContextPtr context,
        const IndexDescription & index_description,
        MergeTreeIndexBloomSlicedParams params_,
        const ITokenizer * tokenizer_,
        MergeTreeIndexTextPreprocessorPtr preprocessor_);

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(
        MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;
    roaring::Roaring bitmapForGranule(MergeTreeIndexGranulePtr granule) const;
    std::vector<size_t> getNeededBitmapPositions(const MergeTreeIndexBloomSlicedParams * actual_params = nullptr) const;
    std::optional<BloomSlicedTokenPredicate> createTokenPredicate(const ActionsDAG::Node & node, ContextPtr context) const;
    String replaceToVirtualColumn(const BloomSlicedTokenPredicate & predicate, const String & index_name);
    BloomSlicedTokenPredicate getTokenPredicateForVirtualColumn(const String & column_name) const;
    std::string getDescription() const override;

private:
    bool traverseAtomNode(const RPNBuilderTreeNode & node, RPNElement & out) const;
    std::optional<BloomSlicedTokenPredicate> predicateFromFunctionNode(const RPNBuilderFunctionTreeNode & function_node) const;
    /// The `preprocess` flag selects the namespace of the returned tokens: with `true` the needle
    /// is transformed with `processConstant` first (stored namespace, used without a lossy
    /// preprocessor); with `false` the tokens are derived from the raw needle (raw namespace, used
    /// to build tombstone token groups under a lossy preprocessor).
    std::vector<String> stringToTokens(const Field & field, bool preprocess) const;
    std::vector<String> stringLikeToTokens(const Field & field, bool preprocess) const;
    std::vector<String> substringToTokens(const Field & field, bool is_prefix, bool is_suffix, bool preprocess) const;
    std::vector<String> substringToTokens(const String & value, bool is_prefix, bool is_suffix, bool preprocess) const;
    std::optional<std::vector<String>> regexpToTokens(const String & regexp, bool preprocess) const;
    std::vector<BloomSlicedTokenGroup> makeTokenGroups(std::vector<String> raw_tokens) const;

    String index_column_name;
    ContextPtr query_context;
    MergeTreeIndexBloomSlicedParams params;
    const ITokenizer * tokenizer;
    MergeTreeIndexTextPreprocessorPtr preprocessor;
    std::vector<RPNElement> rpn;
    std::unordered_map<UInt128, BloomSlicedTokenPredicate> all_token_predicates;
    std::unordered_map<String, BloomSlicedTokenPredicate> virtual_column_to_token_predicate;
};

static constexpr std::string_view BLOOM_SLICED_VIRTUAL_COLUMN_PREFIX = "__bloom_sliced_";
bool isBloomSlicedVirtualColumn(const String & column_name);

class MergeTreeIndexBloomSliced final : public IMergeTreeIndex
{
public:
    MergeTreeIndexBloomSliced(
        StorageMetadataPtr metadata_snapshot_,
        const IndexDescription & index_,
        MergeTreeIndexBloomSlicedParams params_,
        std::unique_ptr<ITokenizer> tokenizer_);

    bool isBloomSlicedIndex() const override { return true; }

    MergeTreeIndexSubstreams getSubstreams() const override;
    MergeTreeIndexFormat getDeserializedFormat(
        const MergeTreeDataPartChecksums & checksums,
        const std::string & relative_path_prefix,
        const IDataPartStorage * storage) const override;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexBloomSlicedParams params;
    std::unique_ptr<ITokenizer> tokenizer;
    MergeTreeIndexTextPreprocessorPtr preprocessor;
};

MergeTreeIndexPtr
bloomSlicedIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & settings);
void bloomSlicedIndexValidator(const IndexDescription & index, bool attach, const MergeTreeSettings & settings);

}
