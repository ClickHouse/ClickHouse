#pragma once

#include <memory>

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/ITokenExtractor.h>


namespace DB
{

struct MergeTreeIndexGranuleFullText final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleFullText(
        const String & index_name_,
        size_t columns_number,
        const BloomFilterParameters & params_);

    ~MergeTreeIndexGranuleFullText() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !has_elems; }

    String index_name;
    BloomFilterParameters params;

    std::vector<BloomFilter> bloom_filters;
    bool has_elems;
};

using MergeTreeIndexGranuleFullTextPtr = std::shared_ptr<MergeTreeIndexGranuleFullText>;

struct MergeTreeIndexAggregatorFullText final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorFullText(
        const Names & index_columns_,
        const String & index_name_,
        const BloomFilterParameters & params_,
        TokenExtractorPtr token_extractor_);

    ~MergeTreeIndexAggregatorFullText() override = default;

    bool empty() const override { return !granule || granule->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

    Names index_columns;
    String index_name;
    BloomFilterParameters params;
    TokenExtractorPtr token_extractor;

    MergeTreeIndexGranuleFullTextPtr granule;
};


class MergeTreeConditionFullText final : public IMergeTreeIndexCondition
{
public:
    MergeTreeConditionFullText(
            const SelectQueryInfo & query_info,
            ContextPtr context,
            const Block & index_sample_block,
            const BloomFilterParameters & params_,
            TokenExtractorPtr token_extactor_);

    ~MergeTreeConditionFullText() override = default;

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
            FUNCTION_HAS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_MULTI_SEARCH,
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
        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS and FUNCTION_MULTI_SEARCH
        size_t key_column;

        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS
        std::unique_ptr<BloomFilter> bloom_filter;

        /// For FUNCTION_IN, FUNCTION_NOT_IN and FUNCTION_MULTI_SEARCH
        std::vector<std::vector<BloomFilter>> set_bloom_filters;

        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<size_t> set_key_position;
    };

    using RPN = std::vector<RPNElement>;

    bool traverseAtomAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out);

    bool traverseASTEquals(
        const String & function_name,
        const ASTPtr & key_ast,
        const DataTypePtr & value_type,
        const Field & value_field,
        RPNElement & out);

    bool getKey(const std::string & key_column_name, size_t & key_column_num);
    bool tryPrepareSetBloomFilter(const ASTs & args, RPNElement & out);

    static bool createFunctionEqualsCondition(
        RPNElement & out, const Field & value, const BloomFilterParameters & params, TokenExtractorPtr token_extractor);

    Names index_columns;
    DataTypes index_data_types;
    BloomFilterParameters params;
    TokenExtractorPtr token_extractor;
    RPN rpn;
    /// Sets from syntax analyzer.
    PreparedSets prepared_sets;
};

class MergeTreeIndexFullText final : public IMergeTreeIndex
{
public:
    MergeTreeIndexFullText(
        const IndexDescription & index_,
        const BloomFilterParameters & params_,
        std::unique_ptr<ITokenExtractor> && token_extractor_)
        : IMergeTreeIndex(index_)
        , params(params_)
        , token_extractor(std::move(token_extractor_)) {}

    ~MergeTreeIndexFullText() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    BloomFilterParameters params;
    /// Function for selecting next token.
    std::unique_ptr<ITokenExtractor> token_extractor;
};

}
