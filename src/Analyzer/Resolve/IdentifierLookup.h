#pragma once

#include <cstddef>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Identifier.h>

namespace DB
{

/// Identifier lookup context
enum class IdentifierLookupContext : uint8_t
{
    EXPRESSION = 0,
    FUNCTION,
    TABLE_EXPRESSION,
};

inline const char * toString(IdentifierLookupContext identifier_lookup_context)
{
    switch (identifier_lookup_context)
    {
        case IdentifierLookupContext::EXPRESSION: return "EXPRESSION";
        case IdentifierLookupContext::FUNCTION: return "FUNCTION";
        case IdentifierLookupContext::TABLE_EXPRESSION: return "TABLE_EXPRESSION";
    }
}

inline const char * toStringLowercase(IdentifierLookupContext identifier_lookup_context)
{
    switch (identifier_lookup_context)
    {
        case IdentifierLookupContext::EXPRESSION: return "expression";
        case IdentifierLookupContext::FUNCTION: return "function";
        case IdentifierLookupContext::TABLE_EXPRESSION: return "table expression";
    }
}

/** Structure that represent identifier lookup during query analysis.
  * Lookup can be in query expression, function, table context.
  */
struct IdentifierLookup
{
    Identifier identifier;
    IdentifierLookupContext lookup_context;

    bool isExpressionLookup() const
    {
        return lookup_context == IdentifierLookupContext::EXPRESSION;
    }

    bool isFunctionLookup() const
    {
        return lookup_context == IdentifierLookupContext::FUNCTION;
    }

    bool isTableExpressionLookup() const
    {
        return lookup_context == IdentifierLookupContext::TABLE_EXPRESSION;
    }

    String dump() const
    {
        return identifier.getFullName() + ' ' + toString(lookup_context);
    }
};

inline bool operator==(const IdentifierLookup & lhs, const IdentifierLookup & rhs)
{
    return lhs.identifier.getFullName() == rhs.identifier.getFullName() && lhs.lookup_context == rhs.lookup_context;
}

[[maybe_unused]] inline bool operator!=(const IdentifierLookup & lhs, const IdentifierLookup & rhs)
{
    return !(lhs == rhs);
}

struct IdentifierLookupHash
{
    size_t operator()(const IdentifierLookup & identifier_lookup) const
    {
        return std::hash<std::string>()(identifier_lookup.identifier.getFullName()) ^ static_cast<uint8_t>(identifier_lookup.lookup_context);
    }
};

enum class IdentifierResolvePlace : UInt8
{
    NONE = 0,
    EXPRESSION_ARGUMENTS,
    ALIASES,
    JOIN_TREE,
    /// Valid only for table lookup
    CTE,
    /// Valid only for table lookup
    DATABASE_CATALOG
};

inline const char * toString(IdentifierResolvePlace resolved_identifier_place)
{
    switch (resolved_identifier_place)
    {
        case IdentifierResolvePlace::NONE: return "NONE";
        case IdentifierResolvePlace::EXPRESSION_ARGUMENTS: return "EXPRESSION_ARGUMENTS";
        case IdentifierResolvePlace::ALIASES: return "ALIASES";
        case IdentifierResolvePlace::JOIN_TREE: return "JOIN_TREE";
        case IdentifierResolvePlace::CTE: return "CTE";
        case IdentifierResolvePlace::DATABASE_CATALOG: return "DATABASE_CATALOG";
    }
}

struct IdentifierResolveScope;

struct IdentifierResolveResult
{
    QueryTreeNodePtr resolved_identifier;
    IdentifierResolveScope * scope = nullptr;
    IdentifierResolvePlace resolve_place = IdentifierResolvePlace::NONE;

    [[maybe_unused]] bool isResolved() const
    {
        return resolve_place != IdentifierResolvePlace::NONE;
    }

    [[maybe_unused]] bool isResolvedFromExpressionArguments() const
    {
        return resolve_place == IdentifierResolvePlace::EXPRESSION_ARGUMENTS;
    }

    [[maybe_unused]] bool isResolvedFromAliases() const
    {
        return resolve_place == IdentifierResolvePlace::ALIASES;
    }

    [[maybe_unused]] bool isResolvedFromJoinTree() const
    {
        return resolve_place == IdentifierResolvePlace::JOIN_TREE;
    }

    [[maybe_unused]] bool isResolvedFromCTEs() const
    {
        return resolve_place == IdentifierResolvePlace::CTE;
    }

    void dump(WriteBuffer & buffer) const
    {
        if (!resolved_identifier)
        {
            buffer << "unresolved";
            return;
        }

        buffer << resolved_identifier->formatASTForErrorMessage() << " place " << toString(resolve_place);
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }
};

struct IdentifierResolveState
{
    size_t count = 1;
};

struct IdentifierResolveContext
{
    /// Allow to check join tree during identifier resolution
    bool allow_to_check_join_tree = true;

    /// Allow to check CTEs during table identifier resolution
    bool allow_to_check_cte = true;

    /// Allow to check parent scopes during identifier resolution
    bool allow_to_check_parent_scopes = true;

    /// Allow to check database catalog during table identifier resolution
    bool allow_to_check_database_catalog = true;

    /// Allow to resolve subquery during identifier resolution
    bool allow_to_resolve_subquery_during_identifier_resolution = true;

    bool allow_to_resolve_result_node = true;

    IdentifierResolveScope * scope_to_resolve_alias_expression = nullptr;

    IdentifierResolveContext resolveAliasesAt(IdentifierResolveScope * scope_to_resolve_alias_expression_) const
    {
        IdentifierResolveContext temp = *this;
        if (!scope_to_resolve_alias_expression)
            temp.scope_to_resolve_alias_expression = scope_to_resolve_alias_expression_;
        return temp;
    }
};

}
