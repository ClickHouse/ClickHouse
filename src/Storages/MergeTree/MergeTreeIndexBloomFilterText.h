#pragma once

#include <memory>

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/ITokenExtractor.h>


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

    const String index_name;
    const BloomFilterParameters params;

    std::vector<BloomFilter> bloom_filters;
    bool has_elems;
};

using MergeTreeIndexGranuleBloomFilterTextPtr = std::shared_ptr<MergeTreeIndexGranuleBloomFilterText>;

struct MergeTreeIndexAggregatorBloomFilterText final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorBloomFilterText(
        const Names & index_columns_,
        const String & index_name_,
        const BloomFilterParameters & params_,
        TokenExtractorPtr token_extractor_);

    ~MergeTreeIndexAggregatorBloomFilterText() override = default;

    bool empty() const override { return !granule || granule->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

    Names index_columns;
    String index_name;
    BloomFilterParameters params;
    TokenExtractorPtr token_extractor;

    MergeTreeIndexGranuleBloomFilterTextPtr granule;
};


class MergeTreeConditionBloomFilterText final : public IMergeTreeIndexCondition
{
public:
    MergeTreeConditionBloomFilterText(
            const ActionsDAG * filter_actions_dag,
            ContextPtr context,
            const Block & index_sample_block,
            const BloomFilterParameters & params_,
            TokenExtractorPtr token_extactor_);

    ~MergeTreeConditionBloomFilterText() override = default;

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
            FUNCTION_MATCH,
            FUNCTION_NOT_IN,
            FUNCTION_MULTI_SEARCH,
            FUNCTION_HAS_ANY,
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
        RPNElement & out, const Field & value, const BloomFilterParameters & params, TokenExtractorPtr token_extractor);

    Names index_columns;
    DataTypes index_data_types;
    BloomFilterParameters params;
    TokenExtractorPtr token_extractor;
    RPN rpn;
};

class MergeTreeIndexBloomFilterText final : public IMergeTreeIndex
{
public:
    MergeTreeIndexBloomFilterText(
        const IndexDescription & index_,
        const BloomFilterParameters & params_,
        std::unique_ptr<ITokenExtractor> && token_extractor_)
        : IMergeTreeIndex(index_)
        , params(params_)
        , token_extractor(std::move(token_extractor_)) {}

    ~MergeTreeIndexBloomFilterText() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexConditionPtr createIndexCondition(
            const ActionsDAG * filter_dag, ContextPtr context) const override;

    BloomFilterParameters params;
    /// Function for selecting next token.
    std::unique_ptr<ITokenExtractor> token_extractor;
};

}
