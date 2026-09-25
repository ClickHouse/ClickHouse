#pragma once

#include <memory>

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/ITokenizer.h>
#include <base/unaligned.h>

#include <array>
#include <cstring>


namespace DB
{

struct MergeTreeIndexGranuleBloomFilterText final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleBloomFilterText(
        const String & index_name_,
        size_t columns_number,
        const BloomFilterParameters & params_);

    ~MergeTreeIndexGranuleBloomFilterText() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !has_elems; }

    size_t memoryUsageBytes() const override;

    const String index_name;
    const BloomFilterParameters params;

    std::vector<BloomFilter> bloom_filters;
    bool has_elems;
};

using MergeTreeIndexGranuleBloomFilterTextPtr = std::shared_ptr<MergeTreeIndexGranuleBloomFilterText>;

/// Short tokens already added to one bloom filter. Adding a token again sets the same bits,
/// so a repeated token can be skipped without hashing it. Tokens repeat heavily within a granule
/// (the same words and n-grams recur in most rows), and hashing dominates building the filter.
/// Only tokens of 1 to 7 bytes are remembered: such a token and its size fit into one 64-bit key,
/// so the lookup is exact. A slot keeps the last token hashed to it; a miss only means adding again.
class BloomFilterAddedTokens
{
public:
    static constexpr size_t max_token_size = 7;
    /// A key holds the token bytes below the size, so a token must be shorter than the key.
    static_assert(max_token_size < sizeof(UInt64));

    /// Remembers the token, which lies within [begin, end). Returns whether it was remembered already.
    ALWAYS_INLINE bool checkAndRemember(const char * token, size_t size, const char * begin, const char * end)
    {
        chassert(size >= 1 && size <= max_token_size);

        /// A whole word is loaded when it lies within the document, otherwise the token is copied.
        const auto token_address = reinterpret_cast<uintptr_t>(token);
        const auto begin_address = reinterpret_cast<uintptr_t>(begin);
        const auto end_address = reinterpret_cast<uintptr_t>(end);

        UInt64 key = 0;
        if (token_address >= begin_address && token_address <= end_address && end_address - token_address >= sizeof(UInt64))
        {
            key = unalignedLoadLittleEndian<UInt64>(token);
        }
        else
        {
            char buf[sizeof(UInt64)] = {};
            memcpy(buf, token, size);
            key = unalignedLoadLittleEndian<UInt64>(buf);
        }

        key &= (1ULL << (8 * size)) - 1;
        key |= static_cast<UInt64>(size) << 56;

        UInt64 & slot = slots[(key * 0x9E3779B97F4A7C15ULL) >> (64 - slots_degree)];
        if (slot == key)
            return true;

        slot = key;
        return false;
    }

    void reset() { slots.fill(0); }

private:
    static constexpr size_t slots_degree = 12;
    /// Zero is never a key, because a key holds the size of its non-empty token.
    std::array<UInt64, 1ULL << slots_degree> slots{};
};

struct MergeTreeIndexAggregatorBloomFilterText final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorBloomFilterText(
        const Names & index_columns_,
        const String & index_name_,
        const BloomFilterParameters & params_,
        TokenizerPtr tokenizer_);

    ~MergeTreeIndexAggregatorBloomFilterText() override = default;

    bool empty() const override { return !granule || granule->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

    /// Adds the tokens of the document to the bloom filter of the index column in the current granule.
    void addTokens(std::string_view document, size_t col);

    Names index_columns;
    String index_name;
    BloomFilterParameters params;

    std::unique_ptr<ITokenizer> owned_tokenizer;
    TokenizerPtr tokenizer;

    MergeTreeIndexGranuleBloomFilterTextPtr granule;

    /// Tokens added to the bloom filters of the current granule, per index column. They are remembered only
    /// after a granule has taken a thousand tokens, so a small granule neither uses nor resets them, and
    /// only while enough of them repeat to pay for the lookups.
    std::vector<BloomFilterAddedTokens> added_tokens;
    size_t tokens_in_granule = 0;
    bool added_tokens_used = false;
    bool remember_tokens = true;
    size_t remembered_lookups = 0;
    size_t remembered_hits = 0;
};


class MergeTreeConditionBloomFilterText final : public IMergeTreeIndexCondition
{
public:
    MergeTreeConditionBloomFilterText(
            const ActionsDAG::Node * predicate,
            ContextPtr context,
            const Block & index_sample_block,
            const BloomFilterParameters & params_,
            TokenizerPtr token_extactor_,
            NameSet columns_shadowing_map_subcolumns_);

    ~MergeTreeConditionBloomFilterText() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;
    std::string getDescription() const override { return ""; }

private:
    struct KeyTuplePositionMapping
    {
        KeyTuplePositionMapping(size_t tuple_index_, size_t key_index_) : tuple_index(tuple_index_), key_index(key_index_) {}

        size_t tuple_index;
        size_t key_index;
    };
    /// Uses RPN like KeyCondition
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_HAS,
            FUNCTION_IN,
            FUNCTION_MATCH,
            FUNCTION_NOT_IN,
            FUNCTION_MULTI_SEARCH,
            FUNCTION_HAS_ANY,
            FUNCTION_HAS_ALL,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement( /// NOLINT
                Function function_ = FUNCTION_UNKNOWN, size_t key_column_ = 0, std::unique_ptr<BloomFilter> && const_bloom_filter_ = nullptr)
                : function(function_), key_column(key_column_), bloom_filter(std::move(const_bloom_filter_)) {}

        Function function = FUNCTION_UNKNOWN;
        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS, FUNCTION_MULTI_SEARCH and FUNCTION_HAS_ANY
        size_t key_column;

        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS
        std::unique_ptr<BloomFilter> bloom_filter;

        /// For FUNCTION_IN, FUNCTION_NOT_IN, FUNCTION_MULTI_SEARCH and FUNCTION_HAS_ANY
        std::vector<std::vector<BloomFilter>> set_bloom_filters;

        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<size_t> set_key_position;
    };

    using RPN = std::vector<RPNElement>;

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out);

    bool traverseTreeEquals(
        const String & function_name,
        const RPNBuilderTreeNode & key_node,
        const DataTypePtr & value_type,
        const Field & value_field,
        RPNElement & out);

    std::optional<size_t> getKeyIndex(const std::string & key_column_name);
    bool tryPrepareSetBloomFilter(const RPNBuilderTreeNode & left_argument, const RPNBuilderTreeNode & right_argument, RPNElement & out);

    static bool createFunctionEqualsCondition(
        RPNElement & out, const Field & value, const BloomFilterParameters & params, TokenizerPtr tokenizer);

    Names index_columns;
    DataTypes index_data_types;
    BloomFilterParameters params;

    std::unique_ptr<ITokenizer> owned_tokenizer;
    TokenizerPtr tokenizer;
    NameSet columns_shadowing_map_subcolumns;

    RPN rpn;
};

class MergeTreeIndexBloomFilterText final : public IMergeTreeIndex
{
public:
    MergeTreeIndexBloomFilterText(
        StorageMetadataPtr metadata_snapshot_,
        const IndexDescription & index_,
        const BloomFilterParameters & params_,
        std::unique_ptr<ITokenizer> && tokenizer_)
        : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
        , params(params_)
        , tokenizer(std::move(tokenizer_)) {}

    ~MergeTreeIndexBloomFilterText() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
            const ActionsDAG::Node * predicate, ContextPtr context) const override;

    BloomFilterParameters params;
    /// Function for selecting next token.
    std::unique_ptr<ITokenizer> tokenizer;
};

}
