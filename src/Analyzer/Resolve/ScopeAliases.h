#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Resolve/IdentifierLookup.h>
#include <Poco/String.h>
#include <base/defines.h>

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

    /// Cloned resolved expressions with aliases that must be removed
    QueryTreeNodes node_to_remove_aliases;

    std::unordered_map<std::string, DataTypePtr> alias_name_to_expression_type;

    /// For case-insensitive mode: lowercase alias -> list of original alias names
    std::unordered_map<std::string, std::vector<std::string>> lowercase_expression_alias_to_originals;
    std::unordered_map<std::string, std::vector<std::string>> lowercase_lambda_alias_to_originals;
    std::unordered_map<std::string, std::vector<std::string>> lowercase_table_alias_to_originals;

    std::unordered_map<std::string, QueryTreeNodePtr> & getAliasMap(IdentifierLookupContext lookup_context)
    {
        switch (lookup_context)
        {
            case IdentifierLookupContext::EXPRESSION: return alias_name_to_expression_node;
            case IdentifierLookupContext::FUNCTION: return alias_name_to_lambda_node;
            case IdentifierLookupContext::TABLE_EXPRESSION: return alias_name_to_table_expression_node;
        }
        UNREACHABLE();
    }

    std::unordered_map<std::string, std::vector<std::string>> & getLowercaseAliasMap(IdentifierLookupContext lookup_context)
    {
        switch (lookup_context)
        {
            case IdentifierLookupContext::EXPRESSION: return lowercase_expression_alias_to_originals;
            case IdentifierLookupContext::FUNCTION: return lowercase_lambda_alias_to_originals;
            case IdentifierLookupContext::TABLE_EXPRESSION: return lowercase_table_alias_to_originals;
        }
        UNREACHABLE();
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
        UNREACHABLE();
    }

    QueryTreeNodePtr * find(IdentifierLookup lookup, FindOption find_option)
    {
        auto & alias_map = getAliasMap(lookup.lookup_context);
        const std::string * key = &getKey(lookup.identifier, find_option);

        auto it = alias_map.find(*key);

        if (it == alias_map.end())
            return {};

        return &it->second;
    }

    const QueryTreeNodePtr * find(IdentifierLookup lookup, FindOption find_option) const
    {
        return const_cast<ScopeAliases *>(this)->find(lookup, find_option);
    }

    const QueryTreeNodePtr * findCaseInsensitive(IdentifierLookup lookup, FindOption find_option, std::vector<std::string> * ambiguous_aliases = nullptr) const
    {
        return const_cast<ScopeAliases *>(this)->findCaseInsensitive(lookup, find_option, ambiguous_aliases);
    }

    /// case-insensitive find, does the same as find but sets ambiguous_aliases if multiple matches exist
    QueryTreeNodePtr * findCaseInsensitive(IdentifierLookup lookup, FindOption find_option, std::vector<std::string> * ambiguous_aliases = nullptr)
    {
        auto & alias_map = getAliasMap(lookup.lookup_context);
        auto & lowercase_map = getLowercaseAliasMap(lookup.lookup_context);

        const std::string & key = getKey(lookup.identifier, find_option);
        String lower_key = Poco::toLower(key);

        auto it = lowercase_map.find(lower_key);
        if (it == lowercase_map.end())
            return {};

        const auto & original_names = it->second;
        if (original_names.size() > 1)
        {
            if (ambiguous_aliases)
                *ambiguous_aliases = original_names;
            return {};
        }

        auto alias_it = alias_map.find(original_names.front());
        if (alias_it == alias_map.end())
            return {};

        return &alias_it->second;
    }

    void registerAliasCaseInsensitive(const std::string & alias_name, IdentifierLookupContext lookup_context)
    {
        auto & lowercase_map = getLowercaseAliasMap(lookup_context);
        String lower_name = Poco::toLower(alias_name);
        lowercase_map[lower_name].push_back(alias_name);
    }
};

}
