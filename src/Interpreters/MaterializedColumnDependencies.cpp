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
    /// Only the nodes here. Their expressions are rewritten and analysed on demand, because this runs
    /// per read task per part and a task usually touches a few of the columns at most.
    for (const auto & column : columns)
        if (isMaterializedColumn(column))
            materialized_columns.emplace(column.name, Column{});
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

const MaterializedColumnDependencies::Column * MaterializedColumnDependencies::analyse(const String & column_name) const
{
    auto it = materialized_columns.find(column_name);
    if (it == materialized_columns.end())
        return nullptr;

    if (it->second.expression)
        return &it->second;

    const auto & source_columns_ = getSourceColumns();

    Column materialized;
    materialized.expression = columns.get(column_name).default_desc.expression->clone();

    /// A MATERIALIZED default may read an ALIAS column, and an ALIAS is computed on read and never
    /// stored. Merely adding aliases to the analysis set would let TreeRewriter resolve the name, but
    /// the expression would then demand a column that is not in any part and the recompute stage would
    /// fail on it, so the reference has to be replaced by what it stands for. `replaceAliasColumnsInQuery`
    /// is the expansion reading an ALIAS goes through, and it carries the declared type with it: for
    /// `a UInt8 ALIAS x` over a `UInt16 x` the substituted expression has to narrow exactly as a read of
    /// `a` does, or the recomputed value differs from the inserted one. Done before the subcolumn
    /// rewrite, so a subcolumn reached through an alias is normalized too.
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

const MaterializedColumnDependencies::Column * MaterializedColumnDependencies::tryGet(const String & column_name) const
{
    return analyse(column_name);
}

bool MaterializedColumnDependencies::willBeRecalculated(const String & column_name, const NameSet & changed_columns) const
{
    if (changed_columns.empty())
        return false;

    /// Checked once per entry into the walk, not once per recursive step.
    if (memo_changed_columns != changed_columns)
    {
        memo_changed_columns = changed_columns;
        will_be_recalculated.clear();
    }

    return willBeRecalculatedImpl(column_name, changed_columns);
}

bool MaterializedColumnDependencies::willBeRecalculatedImpl(const String & column_name, const NameSet & changed_columns) const
{
    /// The insertion doubles as an in-progress marker: a walk that comes back into a column still
    /// being answered reads the `false` left here and stops. `CREATE TABLE` already rejects cyclic
    /// defaults (`CYCLIC_ALIASES`), so this is not reachable today — but it costs nothing, because the
    /// marker is the memo entry itself, and it makes the recursion total without relying on that.
    auto [it, inserted] = will_be_recalculated.emplace(column_name, false);
    if (!inserted)
        return it->second;

    const auto * materialized = analyse(column_name);
    if (!materialized || materialized->reads_ephemeral)
        return false;

    bool result = std::ranges::any_of(materialized->dependencies, [&](const auto & dependency)
    {
        return changed_columns.contains(dependency) || willBeRecalculatedImpl(dependency, changed_columns);
    });

    /// Not through `it`: the recursion above can rehash the map, which invalidates iterators.
    will_be_recalculated[column_name] = result;
    return result;
}

}
