#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Resolve/IdentifierLookup.h>
#include <Analyzer/Resolve/StandardNameMatching.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AMBIGUOUS_IDENTIFIER;
}

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

    QueryTreeNodePtr * find(IdentifierLookup lookup, FindOption find_option, NameMatchMode name_match_mode)
    {
        auto & alias_map = getAliasMap(lookup.lookup_context);

        /// `standard` matching: an unquoted reference resolves through the folded alias namespace.
        /// A double-quoted alias definition is pinned and can only be found by an exact-spelling
        /// lookup; a double-quoted reference falls through to the exact path below.
        auto foldable_name = getFoldableIdentifierSuffix(lookup, 0 /*qualifier_parts*/, name_match_mode);
        if (!foldable_name.empty())
        {
            if (find_option == FindOption::FIRST_NAME)
                foldable_name = IdentifierName({foldable_name.front()});

            if (find_option == FindOption::FULL_NAME || foldable_name.front().isCaseFoldable())
            {
                auto matches = collectFoldedNameMatches(alias_map, foldable_name,
                    [](const String &, const QueryTreeNodePtr & node) { return node->getAliasQuote() == IdentifierPartQuote::DoubleQuoted; });

                if (matches.size() > 1)
                    throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                        "Alias reference '{}' is ambiguous under standard name matching. Candidates: {}",
                        lookup.identifier.getFullName(),
                        fmt::join(matches, ", "));

                if (matches.empty())
                    return {};

                return &alias_map.find(matches.front())->second;
            }
        }

        const std::string * key = &getKey(lookup.identifier, find_option);

        auto it = alias_map.find(*key);

        if (it == alias_map.end())
            return {};

        return &it->second;
    }

    const QueryTreeNodePtr * find(IdentifierLookup lookup, FindOption find_option, NameMatchMode name_match_mode) const
    {
        return const_cast<ScopeAliases *>(this)->find(lookup, find_option, name_match_mode);
    }
};

}
