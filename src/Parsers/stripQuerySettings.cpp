#include <Parsers/stripQuerySettings.h>

#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST.h>

#include <algorithm>
#include <vector>

namespace DB
{

namespace
{

bool isEmptySetQuery(const ASTSetQuery & set_query)
{
    return set_query.changes.empty() && set_query.default_settings.empty() && set_query.query_parameters.empty();
}

/// Erase every entry naming a stripped setting from both lists of one SETTINGS node. Both lists matter:
///  - `changes` holds `name = value` and would re-apply the override on top of the pinned fuzz context;
///  - `default_settings` holds `name = DEFAULT` and would reset a pinned cap back to its (unbounded)
///    default via InterpreterSetQuery::resetSettingsToDefaultValue on re-parse.
/// Erase *all* matches, not just the first: ParserSetQuery appends one entry per occurrence, so a
/// repeated `max_rows_to_read = 0, max_rows_to_read = 0` would otherwise leave the second copy behind.
template <typename Predicate>
void stripNamesFromSetQuery(ASTSetQuery & set_query, Predicate && is_stripped)
{
    std::erase_if(set_query.changes, [&](const SettingChange & change) { return is_stripped(change.name); });
    std::erase_if(set_query.default_settings, [&](const String & name) { return is_stripped(name); });
}

/// Detach `field` from `owner`, tolerating a `field` slot that is not registered in `owner.children`.
/// IAST::reset hard-throws LOGICAL_ERROR ("AST subtree not found in children") in that case, but the
/// server-side AST fuzzer can hand us structurally-invalid ASTs whose SETTINGS slot is desynced from
/// `children` (executeQuery.cpp documents that the fuzzer produces such ASTs and the surrounding code
/// only skips them at format time - this strip runs before that guard). Erase the child if present,
/// then clear the slot unconditionally, so a desynced node is detached instead of aborting the server.
/// `IAST::children` is a boost::container::vector, so use the remove/erase idiom (no std::erase_if).
void eraseChild(IAST & owner, const IAST * child_ptr)
{
    owner.children.erase(
        std::remove_if(
            owner.children.begin(),
            owner.children.end(),
            [&](const ASTPtr & child) { return child.get() == child_ptr; }),
        owner.children.end());
}

void detachChild(IAST & owner, ASTPtr & field)
{
    if (!field)
        return;
    eraseChild(owner, field.get());
    field.reset();
}

/// Raw-pointer overload for owners that hold their SETTINGS slot as a bare pointer (e.g. ASTStorage).
template <typename T>
void detachChild(IAST & owner, T *& field)
{
    if (field == nullptr)
        return;
    eraseChild(owner, field);
    field = nullptr;
}

/// Return the owning `ASTPtr` in `owner.children` whose target is `raw` (as an `ASTSetQuery`), or null
/// if none. An owning child *is* the node's keep-alive, so a match proves `raw` still points at that
/// exact live node, while an address-only test proves nothing: a freed node's address can be handed to
/// an unrelated live one. Compares pointer values only (`child.get() == raw`), never dereferences `raw`.
ASTPtr findOwningChild(const IAST & owner, const IAST * raw)
{
    if (raw == nullptr)
        return nullptr;
    for (const auto & child : owner.children)
        if (child.get() == raw && child->as<ASTSetQuery>())
            return child;
    return nullptr;
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

    auto is_stripped = [&](std::string_view name)
    {
        for (const auto & stripped : setting_names)
            if (stripped == name)
                return true;
        return false;
    };

    /// Strip the named settings from each SETTINGS clause and, if a clause becomes empty, detach it from
    /// its owner. The strip alone is not enough: the owner formatters print `SETTINGS ` whenever the slot
    /// is non-null, so an empty node re-serializes to a bare `SETTINGS` that throws on re-parse (the
    /// fuzzer then silently skips the query instead of running it under the caps).
    ///
    /// This handles exactly the SETTINGS carriers that InterpreterSetQuery::applySettingsFromQuery reads
    /// back onto the query context (the only path that can re-apply a query's own settings over the
    /// pinned fuzz-context caps): the SELECT clause, the INSERT clause, the trailing query clause of any
    /// ASTQueryWithOutput (SELECT-UNION, EXPLAIN, SHOW, CREATE ... AS SELECT, ...), the CREATE storage
    /// clause, and the BACKUP/RESTORE clause. SETTINGS in other positions (engine-only storage settings
    /// already filtered by applySettingsFromQuery, MATERIALIZED VIEW refresh strategy, dictionary layout,
    /// column declarations, standalone SET) do not override the execution caps, so they are deliberately
    /// left untouched - stripping them would only risk the same bare-`SETTINGS` prune problem without
    /// closing any override path. Each owner strips and prunes its own clause in one visit, so a single
    /// traversal suffices.
    ///
    visitAllNodes(
        ast,
        [&](IAST & node)
        {
            if (auto * select_query = node.as<ASTSelectQuery>())
            {
                if (auto settings = select_query->settings())
                    if (auto * set_query = settings->as<ASTSetQuery>())
                    {
                        stripNamesFromSetQuery(*set_query, is_stripped);
                        if (isEmptySetQuery(*set_query))
                            select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, {});
                    }
                return;
            }

            if (auto * insert_query = node.as<ASTInsertQuery>())
            {
                if (insert_query->settings_ast)
                    if (auto * set_query = insert_query->settings_ast->as<ASTSetQuery>())
                    {
                        stripNamesFromSetQuery(*set_query, is_stripped);
                        if (isEmptySetQuery(*set_query))
                            detachChild(*insert_query, insert_query->settings_ast);
                    }
                return;
            }

            if (auto * storage = node.as<ASTStorage>())
            {
                /// `CREATE ... SETTINGS max_rows_to_read = 0` parks the cap here; on the server
                /// applySettingsFromQuery moves the non-engine settings from the storage clause onto the
                /// context, so it must be stripped (and pruned to avoid a bare `SETTINGS`).
                ///
                /// `storage->settings` is a bare `ASTSetQuery *` whose designated owner is
                /// `storage->children` (see ASTStorage::normalizeChildrenOrder), and a fuzzer-mutated child
                /// list can drop that owning intrusive_ptr while leaving the slot set. An owning child there
                /// proves the slot is still that same live node, so strip through it, preserving surviving
                /// engine settings (e.g. `index_granularity`), and detach if it empties. Without one the slot
                /// is unprovable rather than provably freed - a caller may hold the node alive outside
                /// `children` - so clear it unread: dropping a desynced clause whole is the safe direction,
                /// since `ASTStorage::formatImpl` would otherwise serialize `*storage->settings`.
                if (storage->settings)
                {
                    if (auto owning = findOwningChild(*storage, storage->settings))
                    {
                        auto & set_query = owning->as<ASTSetQuery &>();
                        stripNamesFromSetQuery(set_query, is_stripped);
                        if (isEmptySetQuery(set_query))
                            detachChild(*storage, storage->settings);
                    }
                    else
                        storage->settings = nullptr;
                }
                return;
            }

            /// ASTQueryWithOutput is the base of SELECT-UNION, EXPLAIN, SHOW, CREATE, BACKUP, ... and its
            /// (final) formatImpl prints the trailing query `SETTINGS` from `settings_ast`. `as<>` is
            /// exact-type only, so use dynamic_cast to reach the base. EXPLAIN's own `EXPLAIN <kind>
            /// name = value` options live in a separate slot the strip list never touches (and which
            /// formats without a `SETTINGS` keyword), so the base branch covers it. This is not mutually
            /// exclusive with the BACKUP branch below: ASTBackupQuery is an ASTQueryWithOutput whose
            /// inherited `settings_ast` is unused while its core settings live in `settings`.
            if (auto * query_with_output = dynamic_cast<ASTQueryWithOutput *>(&node))
            {
                if (query_with_output->settings_ast)
                    if (auto * set_query = query_with_output->settings_ast->as<ASTSetQuery>())
                    {
                        stripNamesFromSetQuery(*set_query, is_stripped);
                        if (isEmptySetQuery(*set_query))
                            detachChild(*query_with_output, query_with_output->settings_ast);
                    }
            }

            if (auto * backup_query = node.as<ASTBackupQuery>())
            {
                /// BACKUP/RESTORE store their core settings in `settings` (not the inherited
                /// `settings_ast`), and it is not registered in `children`, so the traversal would never
                /// reach it; applySettingsFromQuery's backup branch reads it via extractCoreSettingsFromQuery.
                /// Reach it directly and reset the slot (not via IAST::reset, which expects a child).
                if (backup_query->settings)
                    if (auto * set_query = backup_query->settings->as<ASTSetQuery>())
                    {
                        stripNamesFromSetQuery(*set_query, is_stripped);
                        if (isEmptySetQuery(*set_query))
                            backup_query->settings.reset();
                    }
            }
        });
}

}
