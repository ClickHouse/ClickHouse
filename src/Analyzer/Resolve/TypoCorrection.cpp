#include <Analyzer/Identifier.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Utils.h>

#include <Analyzer/Resolve/IdentifierResolveScope.h>
#include <Analyzer/Resolve/TableExpressionData.h>
#include <Analyzer/Resolve/TypoCorrection.h>

#include <DataTypes/IDataType.h>

namespace DB
{

/// Get valid identifiers for typo correction from compound expression
void TypoCorrection::collectCompoundExpressionValidIdentifiers(
    const Identifier & unresolved_identifier,
    const DataTypePtr & compound_expression_type,
    const Identifier & valid_identifier_prefix,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    IDataType::forEachSubcolumn([&](const auto &, const auto & name, const auto &)
    {
        Identifier subcolumn_indentifier(name);
        size_t new_identifier_size = valid_identifier_prefix.getPartsSize() + subcolumn_indentifier.getPartsSize();

        if (new_identifier_size == unresolved_identifier.getPartsSize())
        {
            auto new_identifier = valid_identifier_prefix;
            for (const auto & part : subcolumn_indentifier)
                new_identifier.push_back(part);

            valid_identifiers_result.insert(std::move(new_identifier));
        }
    }, ISerialization::SubstreamData(compound_expression_type->getDefaultSerialization()));
}

/// Get valid identifiers for typo correction from table expression
void TypoCorrection::collectTableExpressionValidIdentifiers(
    const Identifier & unresolved_identifier,
    const QueryTreeNodePtr & table_expression,
    const AnalysisTableExpressionData & table_expression_data,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    for (const auto & [column_name, column_node] : table_expression_data.column_name_to_column_node)
    {
        Identifier column_identifier(column_name);
        if (unresolved_identifier.getPartsSize() == column_identifier.getPartsSize())
            valid_identifiers_result.insert(column_identifier);

        collectCompoundExpressionValidIdentifiers(unresolved_identifier,
            column_node->getColumnType(),
            column_identifier,
            valid_identifiers_result);

        if (table_expression->hasAlias())
        {
            Identifier column_identifier_with_alias({table_expression->getAlias()});
            for (const auto & column_identifier_part : column_identifier)
                column_identifier_with_alias.push_back(column_identifier_part);

            if (unresolved_identifier.getPartsSize() == column_identifier_with_alias.getPartsSize())
                valid_identifiers_result.insert(column_identifier_with_alias);

            collectCompoundExpressionValidIdentifiers(unresolved_identifier,
                column_node->getColumnType(),
                column_identifier_with_alias,
                valid_identifiers_result);
        }

        if (!table_expression_data.table_name.empty())
        {
            Identifier column_identifier_with_table_name({table_expression_data.table_name});
            for (const auto & column_identifier_part : column_identifier)
                column_identifier_with_table_name.push_back(column_identifier_part);

            if (unresolved_identifier.getPartsSize() == column_identifier_with_table_name.getPartsSize())
                valid_identifiers_result.insert(column_identifier_with_table_name);

            collectCompoundExpressionValidIdentifiers(unresolved_identifier,
                column_node->getColumnType(),
                column_identifier_with_table_name,
                valid_identifiers_result);
        }

        if (!table_expression_data.database_name.empty() && !table_expression_data.table_name.empty())
        {
            Identifier column_identifier_with_table_name_and_database_name({table_expression_data.database_name, table_expression_data.table_name});
            for (const auto & column_identifier_part : column_identifier)
                column_identifier_with_table_name_and_database_name.push_back(column_identifier_part);

            if (unresolved_identifier.getPartsSize() == column_identifier_with_table_name_and_database_name.getPartsSize())
                valid_identifiers_result.insert(column_identifier_with_table_name_and_database_name);

            collectCompoundExpressionValidIdentifiers(unresolved_identifier,
                column_node->getColumnType(),
                column_identifier_with_table_name_and_database_name,
                valid_identifiers_result);
        }
    }
}

/// Get valid identifiers for typo correction from scope without looking at parent scopes
void TypoCorrection::collectScopeValidIdentifiers(
    const Identifier & unresolved_identifier,
    const IdentifierResolveScope & scope,
    bool allow_expression_identifiers,
    bool allow_function_identifiers,
    bool allow_table_expression_identifiers,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    bool identifier_is_short = unresolved_identifier.isShort();
    bool identifier_is_compound = unresolved_identifier.isCompound();

    if (allow_expression_identifiers)
    {
        for (const auto & [name, expression] : scope.aliases.alias_name_to_expression_node)
        {
            assert(expression);
            auto expression_identifier = Identifier(name);
            valid_identifiers_result.insert(expression_identifier);
        }

        if (identifier_is_compound)
        {
            for (const auto & [name, expression_type] : scope.aliases.alias_name_to_expression_type)
            {
                chassert(expression_type);
                auto expression_identifier = Identifier(name);

                collectCompoundExpressionValidIdentifiers(unresolved_identifier,
                    expression_type,
                    expression_identifier,
                    valid_identifiers_result);
            }
        }

        for (const auto & [table_expression, table_expression_data] : scope.table_expression_node_to_data)
        {
            collectTableExpressionValidIdentifiers(unresolved_identifier,
                table_expression,
                table_expression_data,
                valid_identifiers_result);
        }
    }

    if (identifier_is_short)
    {
        if (allow_function_identifiers)
        {
            for (const auto & [name, _] : scope.aliases.alias_name_to_expression_node)
                valid_identifiers_result.insert(Identifier(name));
        }

        if (allow_table_expression_identifiers)
        {
            for (const auto & [name, _] : scope.aliases.alias_name_to_table_expression_node)
                valid_identifiers_result.insert(Identifier(name));
        }
    }

    for (const auto & [argument_name, expression] : scope.expression_argument_name_to_node)
    {
        assert(expression);
        auto expression_node_type = expression->getNodeType();

        if (allow_expression_identifiers && isExpressionNodeType(expression_node_type))
        {
            auto expression_identifier = Identifier(argument_name);
            valid_identifiers_result.insert(expression_identifier);

            auto result_type = getExpressionNodeResultTypeOrNull(expression);

            if (identifier_is_compound && result_type)
            {
                collectCompoundExpressionValidIdentifiers(unresolved_identifier,
                    result_type,
                    expression_identifier,
                    valid_identifiers_result);
            }
        }
        else if (identifier_is_short && allow_function_identifiers && isFunctionExpressionNodeType(expression_node_type))
        {
            valid_identifiers_result.insert(Identifier(argument_name));
        }
        else if (allow_table_expression_identifiers && isTableExpressionNodeType(expression_node_type))
        {
            valid_identifiers_result.insert(Identifier(argument_name));
        }
    }
}

void TypoCorrection::collectScopeWithParentScopesValidIdentifiers(
    const Identifier & unresolved_identifier,
    const IdentifierResolveScope & scope,
    bool allow_expression_identifiers,
    bool allow_function_identifiers,
    bool allow_table_expression_identifiers,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    const IdentifierResolveScope * current_scope = &scope;

    while (current_scope)
    {
        collectScopeValidIdentifiers(unresolved_identifier,
            *current_scope,
            allow_expression_identifiers,
            allow_function_identifiers,
            allow_table_expression_identifiers,
            valid_identifiers_result);

        current_scope = current_scope->parent_scope;
    }
}

std::vector<String> TypoCorrection::collectIdentifierTypoHints(const Identifier & unresolved_identifier, const std::unordered_set<Identifier> & valid_identifiers)
{
    std::vector<String> prompting_strings;
    prompting_strings.reserve(valid_identifiers.size());

    for (const auto & valid_identifier : valid_identifiers)
        prompting_strings.push_back(valid_identifier.getFullName());

    return NamePrompter<1>::getHints(unresolved_identifier.getFullName(), prompting_strings);
}

}
