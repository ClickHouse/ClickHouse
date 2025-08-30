#pragma once
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Interpreters/ITokenExtractor.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Interpreters/GinQueryString.h>

namespace DB
{

class MergeTreeIndexConditionText final : public IMergeTreeIndexCondition, WithContext
{
public:
    MergeTreeIndexConditionText(
        const ActionsDAG::Node * predicate,
        ContextPtr context,
        const Block & index_sample_block,
        TokenExtractorPtr token_extactor_);

    ~MergeTreeIndexConditionText() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;
    const std::vector<String> & getAllSearchTokens() const { return all_search_tokens; }
    bool useBloomFilter() const { return use_bloom_filter; }

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
            /// Atoms
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_MATCH,
            FUNCTION_SEARCH_ANY,
            FUNCTION_SEARCH_ALL,
            /// Can take any value
            FUNCTION_UNKNOWN,
            /// Operators
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        explicit RPNElement(Function function_ = FUNCTION_UNKNOWN, std::unique_ptr<GinQueryString> && gin_query_string_ = nullptr)
            : function(function_), gin_query_string(std::move(gin_query_string_))
        {
        }

        Function function = FUNCTION_UNKNOWN;

        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS
        std::unique_ptr<GinQueryString> gin_query_string;

        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<std::vector<GinQueryString>> gin_query_strings_for_set;

        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<size_t> set_key_position;
    };

    using RPN = std::vector<RPNElement>;

    bool traverseAtomAST(const RPNBuilderTreeNode & node, RPNElement & out);

    bool traverseASTEquals(
        const RPNBuilderFunctionTreeNode & function_node,
        const RPNBuilderTreeNode & index_column_ast,
        const DataTypePtr & value_type,
        const Field & value_field,
        RPNElement & out);

    bool tryPrepareSetGinFilter(const RPNBuilderTreeNode & lhs, const RPNBuilderTreeNode & rhs, RPNElement & out);

    const Block & header;
    TokenExtractorPtr token_extractor;
    RPN rpn;
    PreparedSetsPtr prepared_sets;
    std::vector<String> all_search_tokens;
    bool use_bloom_filter = true;
};

}
