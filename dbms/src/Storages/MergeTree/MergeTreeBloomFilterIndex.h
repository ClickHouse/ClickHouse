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
    explicit MergeTreeBloomFilterIndexGranule(const MergeTreeBloomFilterIndex & index);

    ~MergeTreeBloomFilterIndexGranule() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;

    void deserializeBinary(ReadBuffer & istr) override;

    bool empty() const override { return !has_elems; };
    void update(const Block & block, size_t * pos, size_t limit) override;

    const MergeTreeBloomFilterIndex & index;
    StringBloomFilter bloom_filter;
    bool has_elems;
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
            /*FUNCTION_IN,
            FUNCTION_NOT_IN,*/
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
    };

    using AtomMap = std::unordered_map<std::string, bool(*)(RPNElement & out, const Field & value, const MergeTreeBloomFilterIndex & idx)>;
    using RPN = std::vector<RPNElement>;

    void traverseAST(const ASTPtr & node, const Context & context, Block & block_with_constants);
    bool atomFromAST(const ASTPtr & node, const Context & context, Block & block_with_constants, RPNElement & out);
    bool operatorFromAST(const ASTFunction * func, RPNElement & out);

    bool getKey(const ASTPtr & node, size_t & key_column_num);

    static const AtomMap atom_map;

    const MergeTreeBloomFilterIndex & index;
    RPN rpn;
};

struct TokenExtractor
{
    virtual ~TokenExtractor() = default;
    /// Fast inplace implementation for regular use.
    virtual bool next(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const = 0;
    /// Special implementation for creating bloom filter for LIKE function.
    /// It skips unescaped `%` and `_` and supports escaping symbols, but it is less lightweight.
    virtual bool nextLike(const String & str, size_t * pos, String & out) const = 0;
};

struct NgramTokenExtractor : public TokenExtractor
{
    NgramTokenExtractor(size_t n_) : n(n_) {}

    static String getName() {
        static String name = "ngrambf";
        return name;
    }

    bool next(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const override;
    bool nextLike(const String & str, size_t * pos, String & token) const override;

    size_t n;
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
            std::unique_ptr<TokenExtractor> && tokenExtractorFunc_)
            : IMergeTreeIndex(name_, expr_, columns_, data_types_, header_, granularity_)
            , bloom_filter_size(bloom_filter_size_)
            , bloom_filter_hashes(bloom_filter_hashes_)
            , seed(seed_)
            , tokenExtractorFunc(std::move(tokenExtractorFunc_)) {}

    ~MergeTreeBloomFilterIndex() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    IndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, const Context & context) const override;

    /// Bloom filter size in bytes.
    size_t bloom_filter_size;
    /// Number of bloom filter hash functions.
    size_t bloom_filter_hashes;
    /// Bloom filter seed.
    size_t seed;
    /// Fucntion for selecting next token
    std::unique_ptr<TokenExtractor> tokenExtractorFunc;
};

}
