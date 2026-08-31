#include <Interpreters/MaterializedColumnDependencies.h>

#include <Interpreters/TreeRewriter.h>
#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>

#include <ranges>

namespace DB
{

static bool isMaterializedColumn(const ColumnDescription & column)
{
    return column.default_desc.kind == ColumnDefaultKind::Materialized && column.default_desc.expression;
}

MaterializedColumnDependencies::MaterializedColumnDependencies(const ColumnsDescription & columns_, const ContextPtr & context_)
    : columns(columns_)
    , context(context_)
{
    /// Only the nodes here; their expressions are analysed on demand.
    for (const auto & column : columns)
        if (isMaterializedColumn(column))
            materialized_columns.emplace(column.name, MaterializedDependencyNode{});
}

const NamesAndTypesList & MaterializedColumnDependencies::getSourceColumns() const
{
    if (source_columns)
        return *source_columns;

    /// `getAllPhysical` omits EPHEMERAL columns, but a MATERIALIZED expression may read one and
    /// analysing it without them fails with UNKNOWN_IDENTIFIER.
    source_columns = columns.getAllPhysical();
    for (const auto & column : columns.getEphemeral())
    {
        ephemeral_columns.insert(column.name);
        source_columns->push_back(column);
    }

    return *source_columns;
}

const MaterializedColumnDependencies::MaterializedDependencyNode *
MaterializedColumnDependencies::findNode(const String & column_name) const
{
    auto it = materialized_columns.find(column_name);
    if (it == materialized_columns.end())
        return nullptr;

    if (it->second.expression)
        return &it->second;

    const auto & source_columns_ = getSourceColumns();

    MaterializedDependencyNode materialized;
    materialized.expression = columns.get(column_name).default_desc.expression->clone();

    /// An ALIAS is computed on read and never stored, so a reference to one has to be replaced by what
    /// it stands for, cast to its declared type — resolving the name alone would leave the recompute
    /// stage demanding a column no part holds, and skipping the cast would recompute a value that
    /// differs from the inserted one. Before the subcolumn rewrite, so an alias to one is normalized too.
    replaceAliasColumnsInQuery(materialized.expression, columns, {}, context);

    /// Both the read set and the recompute stage are keyed on top-level columns, so a default over a
    /// subcolumn must be reported as depending on `t`, not on `t.a`.
    replaceSubcolumnsToGetSubcolumnFunctionInQuery(materialized.expression, source_columns_);

    /// `analyze` rewrites in place, so keep `materialized.expression` pristine for callers.
    auto query = materialized.expression->clone();
    materialized.dependencies = TreeRewriter(context).analyze(query, source_columns_)->requiredSourceColumns();
    materialized.reads_ephemeral = std::ranges::any_of(
        materialized.dependencies, [&](const auto & dependency) { return ephemeral_columns.contains(dependency); });

    /// One assignment, so the entry is either untouched or complete.
    it->second = std::move(materialized);
    return &it->second;
}

const Names & MaterializedColumnDependencies::findColumnsToRecalculate(
    const String & column_name, const NameSet & changed_columns) const
{
    static const Names none;

    if (changed_columns.empty())
        return none;

    /// This is the entry into the walk, so the memo is keyed here — once per walk rather than once
    /// per column the recursion below visits.
    if (memo_changed_columns != changed_columns)
    {
        memo_changed_columns = changed_columns;
        will_be_recalculated.clear();
    }

    /// A recalculated column needs all of its dependencies — the stage that rewrites it evaluates the
    /// whole expression, including the parts reading a column no mutation touches.
    if (!willBeRecalculated(column_name, changed_columns))
        return none;

    /// The walk above reached the column, so this is the memoised entry, not a second analysis.
    return findNode(column_name)->dependencies;
}

bool MaterializedColumnDependencies::willBeRecalculated(const String & column_name, const NameSet & changed_columns) const
{
    /// The insertion doubles as an in-progress marker, so a walk that re-enters a column still being
    /// answered stops here. Unreachable today — `CREATE TABLE` rejects cyclic defaults (`CYCLIC_ALIASES`)
    /// — but the marker is the memo entry itself, so making the recursion total costs nothing.
    auto [it, inserted] = will_be_recalculated.emplace(column_name, false);
    if (!inserted)
        return it->second;

    const auto * materialized = findNode(column_name);
    if (!materialized || materialized->reads_ephemeral)
        return false;

    bool result = std::ranges::any_of(materialized->dependencies, [&](const auto & dependency)
    {
        return changed_columns.contains(dependency) || willBeRecalculated(dependency, changed_columns);
    });

    /// Not through `it`: the recursion above can rehash the map, which invalidates iterators.
    will_be_recalculated[column_name] = result;
    return result;
}

}
