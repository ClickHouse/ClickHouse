#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>


namespace DB
{

class MergeTreeBloomFilterIndex;


struct MergeTreeBloomFilterIndexGranule : public IMergeTreeIndexGranule
{
    explicit MergeTreeBloomFilterIndexGranule(
            const MergeTreeBloomFilterIndex & index);

    ~MergeTreeBloomFilterIndexGranule() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    bool empty() const override { return !has_elems; }

    const MergeTreeBloomFilterIndex & index;
    std::vector<StringBloomFilter> bloom_filters;
    bool has_elems;
};

using MergeTreeBloomFilterIndexGranulePtr = std::shared_ptr<MergeTreeBloomFilterIndexGranule>;


struct MergeTreeBloomFilterIndexAggregator : IMergeTreeIndexAggregator
{
    explicit MergeTreeBloomFilterIndexAggregator(const MergeTreeBloomFilterIndex & index);

    ~MergeTreeBloomFilterIndexAggregator() override = default;

    bool empty() const override { return !granule || granule->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

    const MergeTreeBloomFilterIndex & index;
    MergeTreeBloomFilterIndexGranulePtr granule;
};


class BloomFilterCondition : public IIndexCondition
{
public:
    BloomFilterCondition(
            const SelectQueryInfo & query_info,
            const Context & context,
            const MergeTreeBloomFilterIndex & index_);

    ~BloomFilterCondition() override = default;

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;
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
            FUNCTION_LIKE,
            FUNCTION_NOT_LIKE,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement(
            Function function_ = FUNCTION_UNKNOWN, size_t key_column_ = 0, std::unique_ptr<StringBloomFilter> && const_bloom_filter_ = nullptr)
            : function(function_), key_column(key_column_), bloom_filter(std::move(const_bloom_filter_)) {}

        Function function = FUNCTION_UNKNOWN;
        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS, FUNCTION_LIKE, FUNCTION_NOT_LIKE.
        size_t key_column;
        std::unique_ptr<StringBloomFilter> bloom_filter;
        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<std::vector<StringBloomFilter>> set_bloom_filters;
        std::vector<size_t> set_key_position;
    };

    using AtomMap = std::unordered_map<std::string, bool(*)(RPNElement & out, const Field & value, const MergeTreeBloomFilterIndex & idx)>;
    using RPN = std::vector<RPNElement>;

    bool atomFromAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out);

    bool getKey(const ASTPtr & node, size_t & key_column_num);
    bool tryPrepareSetBloomFilter(const ASTs & args, RPNElement & out);

    static const AtomMap atom_map;

    const MergeTreeBloomFilterIndex & index;
    RPN rpn;
    /// Sets from syntax analyzer.
    PreparedSets prepared_sets;
};


/// Interface for string parsers.
struct ITokenExtractor
{
    virtual ~ITokenExtractor() = default;
    /// Fast inplace implementation for regular use.
    /// Gets string (data ptr and len) and start position for extracting next token (state of extractor).
    /// Returns false if parsing is finished, otherwise returns true.
    virtual bool next(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const = 0;
    /// Special implementation for creating bloom filter for LIKE function.
    /// It skips unescaped `%` and `_` and supports escaping symbols, but it is less lightweight.
    virtual bool nextLike(const String & str, size_t * pos, String & out) const = 0;

    virtual bool supportLike() const = 0;
};

/// Parser extracting all ngrams from string.
struct NgramTokenExtractor : public ITokenExtractor
{
    NgramTokenExtractor(size_t n_) : n(n_) {}

    static String getName() { return "ngrambf_v1"; }

    bool next(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const override;
    bool nextLike(const String & str, size_t * pos, String & token) const override;

    bool supportLike() const override { return true; }

    size_t n;
};

/// Parser extracting tokens (sequences of numbers and ascii letters).
struct SplitTokenExtractor : public ITokenExtractor
{
    static String getName() { return "tokenbf_v1"; }

    bool next(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const override;
    bool nextLike(const String & str, size_t * pos, String & token) const override;

    bool supportLike() const override { return true; }
};


class MergeTreeBloomFilterIndex : public IMergeTreeIndex
{
public:
    MergeTreeBloomFilterIndex(
            String name_,
            ExpressionActionsPtr expr_,
            const Names & columns_,
            const DataTypes & data_types_,
            const Block & header_,
            size_t granularity_,
            size_t bloom_filter_size_,
            size_t bloom_filter_hashes_,
            size_t seed_,
            std::unique_ptr<ITokenExtractor> && token_extractor_func_)
            : IMergeTreeIndex(name_, expr_, columns_, data_types_, header_, granularity_)
            , bloom_filter_size(bloom_filter_size_)
            , bloom_filter_hashes(bloom_filter_hashes_)
            , seed(seed_)
            , token_extractor_func(std::move(token_extractor_func_)) {}

    ~MergeTreeBloomFilterIndex() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    IndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, const Context & context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    /// Bloom filter size in bytes.
    size_t bloom_filter_size;
    /// Number of bloom filter hash functions.
    size_t bloom_filter_hashes;
    /// Bloom filter seed.
    size_t seed;
    /// Fucntion for selecting next token.
    std::unique_ptr<ITokenExtractor> token_extractor_func;
};

}
