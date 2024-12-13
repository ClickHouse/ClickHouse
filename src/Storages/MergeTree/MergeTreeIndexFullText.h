#pragma once

#include <Interpreters/GinFilter.h>
#include <Interpreters/ITokenExtractor.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <base/types.h>
#include <memory>

namespace DB
{
struct MergeTreeIndexGranuleFullText final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleFullText(
        const String & index_name_,
        size_t columns_number,
        const GinFilterParameters & params_);

    ~MergeTreeIndexGranuleFullText() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !has_elems; }

    const String index_name;
    const GinFilterParameters params;
    GinFilters gin_filters;
    bool has_elems;
};

using MergeTreeIndexGranuleFullTextPtr = std::shared_ptr<MergeTreeIndexGranuleFullText>;

struct MergeTreeIndexAggregatorFullText final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorFullText(
        GinIndexStorePtr store_,
        const Names & index_columns_,
        const String & index_name_,
        const GinFilterParameters & params_,
        TokenExtractorPtr token_extractor_);

    ~MergeTreeIndexAggregatorFullText() override = default;

    bool empty() const override { return !granule || granule->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

    void addToGinFilter(UInt32 rowID, const char * data, size_t length, GinFilter & gin_filter);

    GinIndexStorePtr store;
    Names index_columns;
    const String index_name;
    const GinFilterParameters params;
    TokenExtractorPtr token_extractor;

    MergeTreeIndexGranuleFullTextPtr granule;
};


class MergeTreeConditionFullText final : public IMergeTreeIndexCondition, WithContext
{
public:
    MergeTreeConditionFullText(
            const ActionsDAG * filter_actions_dag,
            ContextPtr context,
            const Block & index_sample_block,
            const GinFilterParameters & params_,
            TokenExtractorPtr token_extactor_);

    ~MergeTreeConditionFullText() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule([[maybe_unused]]MergeTreeIndexGranulePtr idx_granule) const override
    {
        /// should call mayBeTrueOnGranuleInPart instead
        assert(false);
        return false;
    }
    bool mayBeTrueOnGranuleInPart(MergeTreeIndexGranulePtr idx_granule, [[maybe_unused]] PostingsCacheForStore & cache_store) const;

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
            FUNCTION_MATCH,
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
                Function function_ = FUNCTION_UNKNOWN, size_t key_column_ = 0, std::unique_ptr<GinFilter> && const_gin_filter_ = nullptr)
                : function(function_), key_column(key_column_), gin_filter(std::move(const_gin_filter_)) {}

        Function function = FUNCTION_UNKNOWN;

        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS and FUNCTION_MULTI_SEARCH
        size_t key_column;

        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS
        std::unique_ptr<GinFilter> gin_filter;

        /// For FUNCTION_IN, FUNCTION_NOT_IN and FUNCTION_MULTI_SEARCH
        std::vector<GinFilters> set_gin_filters;

        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<size_t> set_key_position;
    };

    using RPN = std::vector<RPNElement>;

    bool traverseAtomAST(const RPNBuilderTreeNode & node, RPNElement & out);

    bool traverseASTEquals(
        const String & function_name,
        const RPNBuilderTreeNode & key_ast,
        const DataTypePtr & value_type,
        const Field & value_field,
        RPNElement & out);

    bool tryPrepareSetGinFilter(const RPNBuilderTreeNode & lhs, const RPNBuilderTreeNode & rhs, RPNElement & out);

    static bool createFunctionEqualsCondition(
        RPNElement & out, const Field & value, const GinFilterParameters & params, TokenExtractorPtr token_extractor);

    const Block & header;
    GinFilterParameters params;
    TokenExtractorPtr token_extractor;
    RPN rpn;
    /// Sets from syntax analyzer.
    PreparedSetsPtr prepared_sets;
};

class MergeTreeIndexFullText final : public IMergeTreeIndex
{
public:
    MergeTreeIndexFullText(
        const IndexDescription & index_,
        const GinFilterParameters & params_,
        std::unique_ptr<ITokenExtractor> && token_extractor_)
        : IMergeTreeIndex(index_)
        , params(params_)
        , token_extractor(std::move(token_extractor_)) {}

    ~MergeTreeIndexFullText() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexAggregatorPtr createIndexAggregatorForPart(const GinIndexStorePtr & store, const MergeTreeWriterSettings & /*settings*/) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG * filter_actions_dag, ContextPtr context) const override;

    GinFilterParameters params;
    /// Function for selecting next token.
    std::unique_ptr<ITokenExtractor> token_extractor;
};

}
