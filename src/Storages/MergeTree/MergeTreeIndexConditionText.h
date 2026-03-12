#pragma once
#include <memory>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

class TextIndexDictionaryBlockCache;
using TextIndexDictionaryBlockCachePtr = std::shared_ptr<TextIndexDictionaryBlockCache>;

class TextIndexHeaderCache;
using TextIndexHeaderCachePtr = std::shared_ptr<TextIndexHeaderCache>;

class TextIndexPostingsCache;
using TextIndexPostingsCachePtr = std::shared_ptr<TextIndexPostingsCache>;

struct ITokenizer;
using TokenizerPtr = const ITokenizer *;

enum class TextSearchMode : uint8_t
{
    Any,
    All,
};

enum class TextIndexDirectReadMode : uint8_t
{
    /// Do not use direct read.
    None,
    /// Use direct read and remove the original condition.
    Exact,
    /// Use direct read and add a hint to the original condition.
    Hint,
};

/// Represents a single text-search function
struct TextSearchQuery
{
    TextSearchQuery(String function_name_, TextSearchMode search_mode_, TextIndexDirectReadMode direct_read_mode_, std::vector<String> tokens_);

    String function_name;
    TextSearchMode search_mode;
    TextIndexDirectReadMode direct_read_mode;
    std::vector<String> tokens;

    SipHash getHash() const;
};

using TextSearchQueryPtr = std::shared_ptr<TextSearchQuery>;

class MergeTreeIndexTextPreprocessor;
using MergeTreeIndexTextPreprocessorPtr = std::shared_ptr<MergeTreeIndexTextPreprocessor>;

/// Condition for text index.
/// Unlike conditions for other indexes, it can be used after analysis
/// of granules on reading from text index step (see MergeTreeReaderTextIndex)
class MergeTreeIndexConditionText final : public IMergeTreeIndexCondition, public WithContext
{
public:
    MergeTreeIndexConditionText(
        const ActionsDAG::Node * predicate,
        ContextPtr context_,
        const Block & index_sample_block,
        TokenizerPtr tokenizer_,
        MergeTreeIndexTextPreprocessorPtr preprocessor_);

    ~MergeTreeIndexConditionText() override = default;
    static bool isSupportedFunction(const String & function_name);
    TextIndexDirectReadMode getDirectReadMode(const String & function_name) const;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;
    std::string getDescription() const override;

    const std::vector<String> & getAllSearchTokens() const { return all_search_tokens; }
    TextSearchMode getGlobalSearchMode() const { return global_search_mode; }
    const Block & getHeader() const { return header; }

    /// Create text search query for the function node if it is suitable for optimization.
    TextSearchQueryPtr createTextSearchQuery(const ActionsDAG::Node & node) const;
    /// Returns generated virtual column name for the replacement of related function node.
    std::optional<String> replaceToVirtualColumn(const TextSearchQuery & query, const String & index_name);
    TextSearchQueryPtr getSearchQueryForVirtualColumn(const String & column_name) const;

    TextIndexDictionaryBlockCachePtr dictionaryBlockCache() const { return dictionary_block_cache; }
    TextIndexHeaderCachePtr headerCache() const { return header_cache; }
    TextIndexPostingsCachePtr postingsCache() const { return postings_cache; }

    TokenizerPtr getTokenizer() const { return tokenizer; }
    MergeTreeIndexTextPreprocessorPtr getPreprocessor() const { return preprocessor; }

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
            FUNCTION_HAS_ANY_TOKENS,
            FUNCTION_HAS_ALL_TOKENS,
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

    TextIndexDirectReadMode getHintOrNoneMode() const;
    bool traverseMapElementKeyNode(const RPNBuilderFunctionTreeNode & function_node, RPNElement & out) const;
    bool traverseMapElementValueNode(const RPNBuilderTreeNode & index_column_node, const Field & const_value) const;

    std::vector<String> stringToTokens(const Field & field) const;
    std::vector<String> substringToTokens(const Field & field, bool is_prefix, bool is_suffix) const;
    std::vector<String> stringLikeToTokens(const Field & field) const;

    bool tryPrepareSetForTextSearch(const RPNBuilderTreeNode & lhs, const RPNBuilderTreeNode & rhs, const String & function_name, RPNElement & out) const;

    /// Returns true if all tokens must be read for text index analysis
    /// and we cannot exit analysis earlier if some of the tokens are missing in granule.
    /// E.g. "hasAnyTokens(s, 'tokens')" or "hasAllTokens(s, 'tokens1') OR hasAllTokens(s, 'tokens2')""
    static bool requiresReadingAllTokens(const RPNElement & element);

    Block header;
    TokenizerPtr tokenizer;
    RPN rpn;
    PreparedSetsPtr prepared_sets;

    /// Sorted unique tokens from all RPN elements.
    std::vector<String> all_search_tokens;
    /// Search queries from all RPN elements
    std::unordered_map<UInt128, TextSearchQueryPtr> all_search_queries;
    /// Mapping from virtual column (optimized for direct read from text index) to search query.
    std::unordered_map<String, TextSearchQueryPtr> virtual_column_to_search_query;
    /// If global mode is All, then we can exit analysis earlier if any token is missing in granule.
    TextSearchMode global_search_mode = TextSearchMode::All;
    /// Reference preprocessor expression
    MergeTreeIndexTextPreprocessorPtr preprocessor;
    /// Instance of the text index dictionary block cache
    TextIndexDictionaryBlockCachePtr dictionary_block_cache;
    /// Instance of the text index dictionary block cache
    TextIndexHeaderCachePtr header_cache;
    /// Instance of the text index dictionary block cache
    TextIndexPostingsCachePtr postings_cache;
};

static constexpr std::string_view TEXT_INDEX_VIRTUAL_COLUMN_PREFIX = "__text_index_";
bool isTextIndexVirtualColumn(const String & column_name);

}
