#include <Parsers/stripQuerySettings.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST.h>

#include <vector>

namespace DB
{

namespace
{

bool isEmptySetQuery(const ASTPtr & settings)
{
    const auto * set_query = settings ? settings->as<ASTSetQuery>() : nullptr;
    return set_query && set_query->changes.empty() && set_query->default_settings.empty()
        && set_query->query_parameters.empty();
}

/// Detach an empty SETTINGS node from `node` if `node` owns one through a typed slot. The formatters
/// for these owners print `SETTINGS ` whenever the slot is non-null, so an empty node must be removed
/// rather than just emptied, otherwise the query re-serializes to a bare `SETTINGS` that fails to
/// re-parse. Mirrors the owners that re-apply query SETTINGS in
/// InterpreterSetQuery::applySettingsFromQuery.
void pruneEmptySettingsOwner(IAST & node)
{
    if (auto * select_query = node.as<ASTSelectQuery>())
    {
        if (isEmptySetQuery(select_query->settings()))
            select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, {});
        return;
    }

    /// ASTQueryWithOutput is the base of SELECT-UNION, EXPLAIN, SHOW, CHECK, ... and its (final)
    /// formatImpl prints the trailing query `SETTINGS` for all of them from `settings_ast`. EXPLAIN's
    /// own `EXPLAIN <kind> name = value ...` options live in a separate slot that the strip list never
    /// touches (and which formats without a `SETTINGS` keyword), so this base branch is all that is
    /// needed. `as<>` is exact-type only, so use dynamic_cast to reach the base.
    if (auto * query_with_output = dynamic_cast<ASTQueryWithOutput *>(&node))
    {
        if (isEmptySetQuery(query_with_output->settings_ast))
            query_with_output->reset(query_with_output->settings_ast);
    }
    else if (auto * insert_query = node.as<ASTInsertQuery>())
    {
        if (isEmptySetQuery(insert_query->settings_ast))
            insert_query->reset(insert_query->settings_ast);
    }
}

template <typename Visitor>
void visitAllNodes(const ASTPtr & ast, Visitor && visit)
{
    std::vector<IAST *> nodes_to_process{ast.get()};
    while (!nodes_to_process.empty())
    {
        auto * node = nodes_to_process.back();
        nodes_to_process.pop_back();

        visit(*node);

        for (const auto & child : node->children)
            if (child)
                nodes_to_process.push_back(child.get());
    }
}

}

void removeSettingsFromQuery(const ASTPtr & ast, std::span<const std::string_view> setting_names)
{
    if (!ast)
        return;

    /// First strip the settings from every SETTINGS clause, then prune the clauses that became empty.
    /// Two passes: a node owning a SETTINGS clause is visited before that clause, so its emptiness is
    /// only known after the whole tree has been stripped.
    /// Strip both lists: `changes` (`name = value`) and `default_settings` (`name = DEFAULT`). The
    /// latter matters because re-applying the query's SETTINGS runs InterpreterSetQuery::
    /// executeForCurrentContext, which calls resetSettingsToDefaultValue on `default_settings` and
    /// would reset a fuzz-context cap back to its (unbounded) default.
    visitAllNodes(
        ast,
        [&](IAST & node)
        {
            if (auto * set_query = node.as<ASTSetQuery>())
                for (const auto & name : setting_names)
                {
                    set_query->changes.removeSetting(name);
                    std::erase(set_query->default_settings, name);
                }
        });

    visitAllNodes(ast, [](IAST & node) { pruneEmptySettingsOwner(node); });
}

}
