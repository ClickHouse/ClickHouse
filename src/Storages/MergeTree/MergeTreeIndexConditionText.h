#pragma once
#include <memory>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Interpreters/ITokenExtractor.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

static constexpr std::string_view TEXT_INDEX_VIRTUAL_COLUMN_PREFIX = "__text_index_";

enum class TextSearchMode : uint8_t
{
    Any,
    All,
};

/// Represents a single text-search function
struct TextSearchQuery
{
    TextSearchQuery(String function_name_, TextSearchMode mode_, std::vector<String> tokens_)
        : function_name(std::move(function_name_)), mode(std::move(mode_)), tokens(std::move(tokens_))
    {
        std::sort(tokens.begin(), tokens.end());
    }

    String function_name;
    TextSearchMode mode;
    std::vector<String> tokens;

    UInt128 getHash() const;
};

using TextSearchQueryPtr = std::shared_ptr<TextSearchQuery>;


/// Condition for text index.
/// Unlike conditions for other indexes, it can be used after analysis
/// of granules on reading from text index step (see MergeTreeReaderTextIndex)
class MergeTreeIndexConditionText final : public IMergeTreeIndexCondition, WithContext
{
public:
    MergeTreeIndexConditionText(
        const ActionsDAG::Node * predicate,
        ContextPtr context,
        const Block & index_sample_block,
        TokenExtractorPtr token_extactor_);

    ~MergeTreeIndexConditionText() override = default;
    static bool isSupportedFunctionForDirectRead(const String & function_name);
    static bool isSupportedFunction(const String & function_name);

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    const std::vector<String> & getAllSearchTokens() const { return all_search_tokens; }
    bool useBloomFilter() const { return use_bloom_filter; }
    TextSearchMode getGlobalSearchMode() const { return global_search_mode; }
    const Block & getHeader() const { return header; }

    /// Create text search query for the function node if it is suitable for optimization.
    TextSearchQueryPtr createTextSearchQuery(const ActionsDAG::Node & node) const;
    /// Returns generated virtual column name for the replacement of related function node.
    std::optional<String> replaceToVirtualColumn(const TextSearchQuery & query, const String & index_name);
    TextSearchQueryPtr getSearchQueryForVirtualColumn(const String & column_name) const;

private:
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

        Function function = FUNCTION_UNKNOWN;
        std::vector<TextSearchQueryPtr> text_search_queries;
    };

    using RPN = std::vector<RPNElement>;

    bool traverseAtomNode(const RPNBuilderTreeNode & node, RPNElement & out) const;

    bool traverseFunctionNode(
        const RPNBuilderFunctionTreeNode & function_node,
        const RPNBuilderTreeNode & index_column_node,
        const DataTypePtr & value_type,
        const Field & value_field,
        RPNElement & out) const;

    bool tryPrepareSetForTextSearch(const RPNBuilderTreeNode & lhs, const RPNBuilderTreeNode & rhs, const String & function_name, RPNElement & out) const;
    static TextSearchMode getTextSearchMode(const RPNElement & element);

    Block header;
    TokenExtractorPtr token_extractor;
    RPN rpn;
    PreparedSetsPtr prepared_sets;

    /// Sorted unique tokens from all RPN elements.
    std::vector<String> all_search_tokens;
    /// Search qieries from all RPN elements.s
    std::unordered_map<UInt128, TextSearchQueryPtr> all_search_queries;
    /// Counters to generate unique virtual column names.
    std::unordered_map<String, size_t> function_name_to_index;
    /// Mapping from virtual column (optimized for direct read from text index) to search query.
    std::unordered_map<String, TextSearchQueryPtr> virtual_column_to_search_query;
    /// Bloom filter can be disabled for better testing of dictionary analysis.
    bool use_bloom_filter = true;
    /// If global mode is All, then we can exit analysis earlier if any token is missing in granule.
    TextSearchMode global_search_mode = TextSearchMode::All;
};

}
