#include <Storages/System/extractTablesFilter.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Functions/IFunction.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/misc.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/typeid_cast.h>

#include <stack>

namespace DB
{

namespace
{

/// How many names an `IN` list may hold before enumerating everything becomes the cheaper option.
constexpr size_t MAX_NAMES = 10000;

/// Unwrap ALIAS nodes to reach the underlying node.
const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children[0];
    return node;
}

/// Try to read a constant string from `node` and return its single value.
/// Unwraps aliases and reads the value via `ColumnConst::getField`, which works
/// even for a `ColumnConst` of logical size 0 (a "pure" constant, as produced by
/// the analyzer) - unlike `column[0]`, which an `empty()` check has to guard.
std::optional<String> tryReadConstString(const ActionsDAG::Node * node)
{
    node = skipAliases(node);
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

/// Extract a LIKE pattern from a top-level conjunct: `col LIKE '…'`, or its analyzer rewrite
/// `startsWith(col, '…')`.
TablesFilter extractLikeFilter(const ActionsDAG::Node * predicate, const String & table_name_column)
{
    /// Collect top-level conjuncts.
    std::vector<const ActionsDAG::Node *> conjuncts;
    const auto * node = skipAliases(predicate);

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

    for (const auto * conjunct : conjuncts)
    {
        conjunct = skipAliases(conjunct);

        if (conjunct->type != ActionsDAG::ActionType::FUNCTION
            || !conjunct->function_base
            || conjunct->children.size() != 2)
            continue;

        const auto & fn_name = conjunct->function_base->getName();
        if (fn_name != "like" && fn_name != "startsWith")
            continue;

        /// Neither function is symmetric: only the pattern on the right constrains the column.
        /// The column reads as an INPUT with the column's name once aliases are unwrapped.
        /// (A constant carries `column`; the column reference does not.)
        const auto * lhs = skipAliases(conjunct->children[0]);
        if (!lhs || lhs->result_name != table_name_column || lhs->column)
            continue;

        auto literal = tryReadConstString(conjunct->children[1]);
        if (!literal)
            continue;

        /// `startsWith`'s literal is a plain prefix, so escape it and append `%` to recover
        /// the equivalent LIKE pattern.
        return TablesFilter::createLike(fn_name == "like" ? std::move(*literal) : escapeForLikeLiteral(*literal) + "%");
    }

    return {};
}

/// `name IN (SELECT …)` reaches filter analysis with its set not built yet: the subquery only runs
/// when the pipeline does, and until then `evaluateExpressionOverConstantCondition` sees a
/// `ColumnSet` it cannot read and gives up - so the enumeration degenerates to the full walk this
/// is meant to avoid. Build such a set here, the way `system.users` and the `MergeTree` key
/// condition already do. The subquery has to run for the filter anyway and its result is kept in
/// the set, so nothing is executed twice.
void buildSetsForInSubqueries(const ActionsDAG::Node * predicate, const ContextPtr & context)
{
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::stack<const ActionsDAG::Node *> nodes;
    nodes.push(predicate);

    while (!nodes.empty())
    {
        const auto * node = nodes.top();
        nodes.pop();
        if (!node || !visited.insert(node).second)
            continue;

        for (const auto * child : node->children)
            nodes.push(child);

        if (node->type != ActionsDAG::ActionType::FUNCTION
            || !node->function_base
            || node->children.size() != 2)
            continue;

        /// A `GLOBAL IN` set is deliberately left alone: `ReadFromRemote` has to attach the external
        /// table to it first, and building it here would leave it created without explicit elements.
        if (!functionIsInOperator(node->function_base->getName()))
            continue;

        const auto * set_node = skipAliases(node->children[1]);
        if (!set_node || !set_node->column)
            continue;

        const IColumn * column = set_node->column.get();
        if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
            column = &column_const->getDataColumn();

        const auto * column_set = typeid_cast<const ColumnSet *>(column);
        if (!column_set)
            continue;

        auto future_set = column_set->getData();
        if (!future_set || future_set->get())
            continue;

        /// The build is a no-op once the subquery plan has been moved out of the set (that happens
        /// during plan optimization, in `DelayedCreatingSetsStep::makePlansForSets`); then the set
        /// stays unbuilt, the extraction below returns nothing, and the enumeration is not narrowed.
        if (auto * set_from_subquery = typeid_cast<FutureSetFromSubquery *>(future_set.get()))
            set_from_subquery->buildOrderedSetInplace(context);
    }
}

}

TablesFilter extractTablesFilter(const ActionsDAG::Node * predicate, const String & table_name_column, const ContextPtr & context)
{
    if (!predicate)
        return {};

    buildSetsForInSubqueries(predicate, context);

    /// An exact set of names is the most useful thing a database can be told, so look for it
    /// first; it also subsumes the plain `col = '…'` case.
    if (auto names = VirtualColumnUtils::extractConstantStringValuesForColumn(predicate, table_name_column, context, MAX_NAMES))
        return TablesFilter::createIn(*names);

    return extractLikeFilter(predicate, table_name_column);
}

std::function<bool(const String &)> extractNameFilter(
    const ActionsDAG::Node * predicate, const String & column_name, const ContextPtr & context)
{
    if (!predicate)
        return {};

    buildSetsForInSubqueries(predicate, context);

    auto names = VirtualColumnUtils::extractConstantStringValuesForColumn(predicate, column_name, context, MAX_NAMES);
    if (!names)
        return {};

    return [name_set = std::make_shared<NameSet>(names->begin(), names->end())](const String & name)
    {
        return name_set->contains(name);
    };
}

}
