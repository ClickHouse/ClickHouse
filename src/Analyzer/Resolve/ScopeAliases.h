#pragma once

#include <unordered_set>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Resolve/IdentifierLookup.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

namespace DB
{

struct ScopeAliases
{
    /// Alias name to query expression node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_expression_node;

    /// Alias name to lambda node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_lambda_node;

    /// Alias name to table expression node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_table_expression_node;

    /// Nodes with duplicated aliases
    QueryTreeNodes nodes_with_duplicated_aliases;

    /// Alias names that were looked up during identifier resolution in this scope.
    /// Multiple expressions with the same alias are an error only if the alias is actually used:
    /// unused conflicting aliases are allowed, because aliases also give names to expressions
    /// without being referenced, e.g. to tuple elements in SELECT tuple(1 AS x), tuple(2 AS x)
    /// (see the setting enable_named_columns_in_function_tuple).
    /// Mutable because lookups are recorded through the const find overload as well.
    mutable std::unordered_set<std::string> used_alias_names;

    /// Cloned resolved expressions with aliases that must be removed
    QueryTreeNodes node_to_remove_aliases;

    UnorderedMapWithMemoryTracking<std::string, DataTypePtr> alias_name_to_expression_type;

    std::unordered_map<std::string, QueryTreeNodePtr> & getAliasMap(IdentifierLookupContext lookup_context)
    {
        switch (lookup_context)
        {
            case IdentifierLookupContext::EXPRESSION: return alias_name_to_expression_node;
            case IdentifierLookupContext::FUNCTION: return alias_name_to_lambda_node;
            case IdentifierLookupContext::TABLE_EXPRESSION: return alias_name_to_table_expression_node;
        }
    }

    enum class FindOption
    {
        FIRST_NAME,
        FULL_NAME,
    };

    const std::string & getKey(const Identifier & identifier, FindOption find_option)
    {
        switch (find_option)
        {
            case FindOption::FIRST_NAME: return identifier.front();
            case FindOption::FULL_NAME: return identifier.getFullName();
        }
    }

    QueryTreeNodePtr * find(IdentifierLookup lookup, FindOption find_option)
    {
        auto & alias_map = getAliasMap(lookup.lookup_context);
        const std::string * key = &getKey(lookup.identifier, find_option);

        auto it = alias_map.find(*key);

        if (it == alias_map.end())
            return {};

        used_alias_names.insert(*key);
        return &it->second;
    }

    const QueryTreeNodePtr * find(IdentifierLookup lookup, FindOption find_option) const
    {
        return const_cast<ScopeAliases *>(this)->find(lookup, find_option);
    }
};

}
