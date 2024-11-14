#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Resolve/IdentifierLookup.h>

namespace DB
{

struct ScopeAliases
{
    /// Alias name to query expression node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_expression_node_before_group_by;
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_expression_node_after_group_by;

    std::unordered_map<std::string, QueryTreeNodePtr> * alias_name_to_expression_node = nullptr;

    /// Alias name to lambda node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_lambda_node;

    /// Alias name to table expression node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_table_expression_node;

    /// Expressions like `x as y` where we can't say whether it's a function, expression or table.
    std::unordered_map<std::string, Identifier> transitive_aliases;

    /// Nodes with duplicated aliases
    std::unordered_set<QueryTreeNodePtr> nodes_with_duplicated_aliases;
    std::vector<QueryTreeNodePtr> cloned_nodes_with_duplicated_aliases;

    /// Names which are aliases from ARRAY JOIN.
    /// This is needed to properly qualify columns from matchers and avoid name collision.
    std::unordered_set<std::string> array_join_aliases;

    std::unordered_map<std::string, QueryTreeNodePtr> & getAliasMap(IdentifierLookupContext lookup_context)
    {
        switch (lookup_context)
        {
            case IdentifierLookupContext::EXPRESSION: return *alias_name_to_expression_node;
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

        if (it != alias_map.end())
            return &it->second;

        if (lookup.lookup_context == IdentifierLookupContext::TABLE_EXPRESSION)
            return {};

        while (it == alias_map.end())
        {
            auto jt = transitive_aliases.find(*key);
            if (jt == transitive_aliases.end())
                return {};

            const auto & new_key = getKey(jt->second, find_option);
            /// Ignore potential cyclic aliases.
            if (new_key == *key)
                return {};

            key = &new_key;
            it = alias_map.find(*key);
        }

        return &it->second;
    }

    const QueryTreeNodePtr * find(IdentifierLookup lookup, FindOption find_option) const
    {
        return const_cast<ScopeAliases *>(this)->find(lookup, find_option);
    }
};

}
