#include <Storages/System/extractTableNameFilter.h>

#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace
{

/// Try to read a constant string from `node` and return its single value.
/// Unwraps aliases and reads the value via `ColumnConst::getField`, which works
/// even for a `ColumnConst` of logical size 0 (a "pure" constant, as produced by
/// the analyzer) — unlike `column[0]`, which an `empty()` check has to guard.
std::optional<String> tryReadConstString(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children[0];
    if (!node || !node->column)
        return {};
    const IColumn * column = node->column.get();
    /// Unwrap `ColumnConst` to its single-row data column. This reads the value
    /// even for a `ColumnConst` of logical size 0 (the analyzer's "pure" constant).
    if (const auto * const_column = typeid_cast<const ColumnConst *>(column))
        column = &const_column->getDataColumn();
    if (column->empty())
        return {};
    Field field = (*column)[0];
    if (field.getType() != Field::Types::String)
        return {};
    return field.safeGet<String>();
}

/// Unwrap ALIAS nodes to reach the underlying node.
const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children[0];
    return node;
}

/// Escape SQL LIKE wildcards (`%`, `_`) and the escape char (`\`) so a literal
/// prefix (e.g. from `startsWith`) becomes an equivalent LIKE pattern.
String escapeForLikeLiteral(const String & s)
{
    String result;
    result.reserve(s.size());
    for (char c : s)
    {
        if (c == '%' || c == '_' || c == '\\')
            result += '\\';
        result += c;
    }
    return result;
}

}

TablesFilter extractTableNameFilter(const ActionsDAG::Node * predicate, const String & column_name)
{
    if (!predicate)
        return {};

    /// Collect top-level conjuncts.
    std::vector<const ActionsDAG::Node *> conjuncts;
    const auto * node = predicate;
    while (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children[0];

    if (node->type == ActionsDAG::ActionType::FUNCTION
        && node->function_base
        && node->function_base->getName() == "and")
    {
        for (const auto * child : node->children)
            conjuncts.push_back(child);
    }
    else
    {
        conjuncts.push_back(node);
    }

    TablesFilter like_filter;
    for (const auto * conjunct : conjuncts)
    {
        while (conjunct->type == ActionsDAG::ActionType::ALIAS && !conjunct->children.empty())
            conjunct = conjunct->children[0];

        if (conjunct->type != ActionsDAG::ActionType::FUNCTION
            || !conjunct->function_base
            || conjunct->children.size() != 2)
            continue;

        const auto & fn_name = conjunct->function_base->getName();

        const auto * lhs = skipAliases(conjunct->children[0]);
        const auto * rhs = skipAliases(conjunct->children[1]);

        /// The filtered column reads as an INPUT once aliases are unwrapped.
        /// (A constant carries `column`; the column reference does not.)
        auto is_name_column = [&](const ActionsDAG::Node * n)
        {
            return n && n->result_name == column_name && !n->column;
        };
        const bool lhs_is_name = is_name_column(lhs);
        const bool rhs_is_name = is_name_column(rhs);
        if (!lhs_is_name && !rhs_is_name)
            continue;

        if (fn_name == "equals")
        {
            /// `equals` is symmetric (literal either side); prefer it — most selective.
            if (auto literal = tryReadConstString(lhs_is_name ? rhs : lhs))
                return {TablesFilter::Kind::Equals, std::move(*literal), {}};
        }
        else if (fn_name == "like")
        {
            /// Not symmetric: only `name LIKE 'pattern'` (name on lhs) constrains `name`.
            /// Keep the first such pattern if no `equals` is found.
            if (lhs_is_name && like_filter.kind == TablesFilter::Kind::None)
            {
                if (auto literal = tryReadConstString(rhs))
                {
                    like_filter.kind = TablesFilter::Kind::Like;
                    like_filter.pattern = std::move(*literal);
                }
            }
        }
        else if (fn_name == "startsWith")
        {
            /// Analyzer rewrite of a perfect-prefix `name LIKE 'prefix%'`. The literal
            /// is a plain prefix, so escape it and append `%` to recover the LIKE pattern.
            if (lhs_is_name && like_filter.kind == TablesFilter::Kind::None)
            {
                if (auto literal = tryReadConstString(rhs))
                {
                    like_filter.kind = TablesFilter::Kind::Like;
                    like_filter.pattern = escapeForLikeLiteral(*literal) + "%";
                }
            }
        }
        else if (fn_name == "notLike")
        {
            if (lhs_is_name && like_filter.exclude_pattern.empty())
            {
                if (auto literal = tryReadConstString(rhs))
                    like_filter.exclude_pattern = std::move(*literal);
            }
        }
    }

    return like_filter;
}

}
