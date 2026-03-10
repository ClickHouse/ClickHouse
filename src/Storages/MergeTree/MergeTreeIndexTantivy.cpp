#include "MergeTreeIndexTantivy.h"

#if USE_TANTIVY

#include <Columns/ColumnString.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <tantivy_search.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

// ============================================================================
// MergeTreeIndexGranuleTantivy
// ============================================================================

MergeTreeIndexGranuleTantivy::MergeTreeIndexGranuleTantivy(const String & index_name_, const String & column_name_)
    : index_name(index_name_), column_name(column_name_)
{
}

void MergeTreeIndexGranuleTantivy::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty tantivy index {}", backQuote(index_name));

    writeVarUInt(serialized_index.size(), ostr);
    ostr.write(reinterpret_cast<const char *>(serialized_index.data()), serialized_index.size());
}

void MergeTreeIndexGranuleTantivy::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    size_t size = 0;
    readVarUInt(size, istr);

    if (size == 0)
    {
        has_data = false;
        return;
    }

    serialized_index.resize(size);
    istr.readStrict(reinterpret_cast<char *>(serialized_index.data()), size);
    has_data = true;
}

// ============================================================================
// MergeTreeIndexAggregatorTantivy
// ============================================================================

MergeTreeIndexAggregatorTantivy::MergeTreeIndexAggregatorTantivy(const String & index_name_, const String & column_name_)
    : index_name(index_name_)
    , column_name(column_name_)
    , granule(std::make_shared<MergeTreeIndexGranuleTantivy>(index_name_, column_name_))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorTantivy::getGranuleAndReset()
{
    // Finalize the current granule if we have data
    if (index_initialized && !index_data.empty())
    {
        auto committed = tantivy_commit(rust::Slice<const uint8_t>(index_data.data(), index_data.size()));
        granule->serialized_index.assign(committed.begin(), committed.end());
        granule->has_data = true;
    }

    // Create a new empty granule and swap with the current one
    auto new_granule = std::make_shared<MergeTreeIndexGranuleTantivy>(index_name, column_name);
    new_granule.swap(granule);

    // Reset state for next granule
    index_data.clear();
    index_initialized = false;
    current_row_id = 0;

    return new_granule;
}

void MergeTreeIndexAggregatorTantivy::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index aggregator position is out of bounds");

    // Initialize index if not done
    if (!index_initialized)
    {
        auto created = tantivy_create_index();
        index_data.assign(created.begin(), created.end());
        index_initialized = true;
    }

    // Get the text column
    const auto & column = block.getByName(column_name).column;
    const auto * col_str = typeid_cast<const ColumnString *>(column.get());

    if (!col_str)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tantivy index requires String column");

    size_t rows_read = 0;
    size_t current_pos = *pos;

    while (rows_read < limit && current_pos < block.rows())
    {
        const auto text = col_str->getDataAt(current_pos);
        std::string text_str(text.data(), text.size());

        auto updated = tantivy_add_document(
            rust::Slice<const uint8_t>(index_data.data(), index_data.size()),
            current_row_id,
            text_str);

        index_data.assign(updated.begin(), updated.end());

        ++current_row_id;
        ++current_pos;
        ++rows_read;
    }

    // Mark that we have data after processing rows
    if (rows_read > 0)
        granule->has_data = true;

    *pos = current_pos;
}

// ============================================================================
// MergeTreeConditionTantivy
// ============================================================================

MergeTreeConditionTantivy::MergeTreeConditionTantivy(
    const ActionsDAG::Node * predicate,
    ContextPtr context,
    const Block & index_sample_block,
    const String & column_name_)
    : column_name(column_name_)
{
    /// Extract data types from the index sample block
    for (const auto & col : index_sample_block)
        index_data_types.push_back(col.type);

    if (!predicate)
    {
        rpn.emplace_back(RPNElement::ALWAYS_TRUE);
        return;
    }

    /// Build RPN from the predicate
    RPNBuilder<RPNElement> builder(
        predicate,
        context,
        [this](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::move(builder).extractRPN();
}

bool MergeTreeConditionTantivy::alwaysUnknownOrTrue() const
{
    /// Check if the RPN contains any useful function
    static const std::unordered_set<RPNElement::Function> matching_functions = {
        RPNElement::FUNCTION_EQUALS,
        RPNElement::FUNCTION_HAS_TOKEN,
        RPNElement::FUNCTION_HAS_TOKEN_OR,
    };

    return rpnEvaluatesAlwaysUnknownOrTrue(rpn, matching_functions);
}

bool MergeTreeConditionTantivy::mayBeTrueOnGranule(
    MergeTreeIndexGranulePtr idx_granule,
    const UpdatePartialDisjunctionResultFn & /*update_partial_disjunction_result_fn*/) const
{
    const auto * granule = typeid_cast<const MergeTreeIndexGranuleTantivy *>(idx_granule.get());
    if (!granule || !granule->has_data || granule->serialized_index.empty())
        return true; // Can't determine, assume it might match

    /// Evaluate the RPN against the Tantivy index
    std::vector<bool> rpn_stack;

    for (const auto & element : rpn)
    {
        switch (element.function)
        {
            case RPNElement::FUNCTION_EQUALS:
            case RPNElement::FUNCTION_HAS_TOKEN:
            {
                if (element.search_terms.empty())
                {
                    rpn_stack.push_back(true);
                    break;
                }

                try
                {
                    auto row_ids = tantivy_search_term(
                        rust::Slice<const uint8_t>(granule->serialized_index.data(), granule->serialized_index.size()),
                        element.search_terms[0]);
                    rpn_stack.push_back(!row_ids.empty());
                }
                catch (...)
                {
                    rpn_stack.push_back(true); // On error, assume it might match
                }
                break;
            }

            case RPNElement::FUNCTION_HAS_TOKEN_OR:
            {
                if (element.search_terms.empty())
                {
                    rpn_stack.push_back(true);
                    break;
                }

                try
                {
                    std::vector<std::string> terms(element.search_terms.begin(), element.search_terms.end());

                    rust::Vec<uint64_t> row_ids;
                    if (element.use_and_logic)
                    {
                        row_ids = tantivy_search_terms_and(
                            rust::Slice<const uint8_t>(granule->serialized_index.data(), granule->serialized_index.size()),
                            terms);
                    }
                    else
                    {
                        row_ids = tantivy_search_terms_or(
                            rust::Slice<const uint8_t>(granule->serialized_index.data(), granule->serialized_index.size()),
                            terms);
                    }
                    rpn_stack.push_back(!row_ids.empty());
                }
                catch (...)
                {
                    rpn_stack.push_back(true);
                }
                break;
            }

            case RPNElement::FUNCTION_NOT_EQUALS:
                rpn_stack.push_back(true); // NOT queries always potentially match
                break;

            case RPNElement::FUNCTION_UNKNOWN:
                rpn_stack.push_back(true);
                break;

            case RPNElement::FUNCTION_NOT:
            {
                if (rpn_stack.empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "RPN stack underflow in NOT");
                rpn_stack.back() = !rpn_stack.back();
                break;
            }

            case RPNElement::FUNCTION_AND:
            {
                if (rpn_stack.size() < 2)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "RPN stack underflow in AND");
                auto rhs = rpn_stack.back();
                rpn_stack.pop_back();
                rpn_stack.back() = rpn_stack.back() && rhs;
                break;
            }

            case RPNElement::FUNCTION_OR:
            {
                if (rpn_stack.size() < 2)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "RPN stack underflow in OR");
                auto rhs = rpn_stack.back();
                rpn_stack.pop_back();
                rpn_stack.back() = rpn_stack.back() || rhs;
                break;
            }

            case RPNElement::ALWAYS_TRUE:
                rpn_stack.push_back(true);
                break;

            case RPNElement::ALWAYS_FALSE:
                rpn_stack.push_back(false);
                break;
        }
    }

    return rpn_stack.empty() || rpn_stack.back();
}

bool MergeTreeConditionTantivy::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
{
    if (!node.isFunction())
        return false;

    const auto function_name = node.toFunctionNode().getFunctionName();
    const auto & args = node.toFunctionNode();

    /// Handle hasToken(column, 'term')
    if (function_name == "hasToken" && args.getArgumentsSize() == 2)
    {
        auto col_node = args.getArgumentAt(0);
        auto val_node = args.getArgumentAt(1);

        if (col_node.isConstant() || !val_node.isConstant())
            return false;

        auto col_name = col_node.getColumnName();
        if (col_name != column_name)
            return false;

        Field value;
        DataTypePtr value_type;
        if (!val_node.tryGetConstant(value, value_type))
            return false;

        if (value.getType() != Field::Types::String)
            return false;

        out.function = RPNElement::FUNCTION_HAS_TOKEN;
        out.search_terms.push_back(value.safeGet<String>());
        return true;
    }

    /// Handle equals(column, 'value') or column = 'value'
    if ((function_name == "equals" || function_name == "=") && args.getArgumentsSize() == 2)
    {
        auto col_node = args.getArgumentAt(0);
        auto val_node = args.getArgumentAt(1);

        if (col_node.isConstant() || !val_node.isConstant())
            return false;

        auto col_name = col_node.getColumnName();
        if (col_name != column_name)
            return false;

        Field value;
        DataTypePtr value_type;
        if (!val_node.tryGetConstant(value, value_type))
            return false;

        if (value.getType() != Field::Types::String)
            return false;

        out.function = RPNElement::FUNCTION_EQUALS;
        out.search_terms.push_back(value.safeGet<String>());
        return true;
    }

    /// Handle LIKE patterns (basic support)
    if (function_name == "like" && args.getArgumentsSize() == 2)
    {
        auto col_node = args.getArgumentAt(0);
        auto val_node = args.getArgumentAt(1);

        if (col_node.isConstant() || !val_node.isConstant())
            return false;

        auto col_name = col_node.getColumnName();
        if (col_name != column_name)
            return false;

        Field value;
        DataTypePtr value_type;
        if (!val_node.tryGetConstant(value, value_type))
            return false;

        if (value.getType() != Field::Types::String)
            return false;

        /// For LIKE '%term%', extract the term
        String pattern = value.safeGet<String>();
        if (pattern.size() >= 2 && pattern.front() == '%' && pattern.back() == '%')
        {
            String term = pattern.substr(1, pattern.size() - 2);
            /// Only use if term doesn't contain wildcards
            if (term.find('%') == String::npos && term.find('_') == String::npos)
            {
                out.function = RPNElement::FUNCTION_HAS_TOKEN;
                out.search_terms.push_back(term);
                return true;
            }
        }
    }

    return false;
}

// ============================================================================
// MergeTreeIndexTantivy
// ============================================================================

MergeTreeIndexTantivy::MergeTreeIndexTantivy(const IndexDescription & index_)
    : IMergeTreeIndex(index_)
{
    if (index.column_names.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Tantivy index requires at least one column");

    if (index.column_names.size() > 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Tantivy index currently supports only one column");

    column_name = index.column_names[0];
}

MergeTreeIndexGranulePtr MergeTreeIndexTantivy::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleTantivy>(index.name, column_name);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexTantivy::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorTantivy>(index.name, column_name);
}

MergeTreeIndexConditionPtr MergeTreeIndexTantivy::createIndexCondition(
    const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionTantivy>(
        predicate,
        context,
        index.sample_block,
        column_name);
}

// ============================================================================
// Factory functions
// ============================================================================

MergeTreeIndexPtr tantivyIndexCreator(const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexTantivy>(index);
}

void tantivyIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    if (index.column_names.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Tantivy index requires at least one column");

    if (index.column_names.size() > 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Tantivy index currently supports only one column");

    // Validate that the column is of String type
    for (const auto & col : index.sample_block)
    {
        if (!isString(col.type))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Tantivy index requires String column, got {}", col.type->getName());
    }
}

}

#endif
