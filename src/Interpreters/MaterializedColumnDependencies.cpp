#include <Interpreters/MaterializedColumnDependencies.h>

#include <Interpreters/TreeRewriter.h>
#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnsDescription.h>

#include <ranges>

namespace DB
{

static bool isMaterializedColumn(const ColumnDescription & column)
{
    return column.default_desc.kind == ColumnDefaultKind::Materialized && column.default_desc.expression;
}

MaterializedColumnDependencies::MaterializedColumnDependencies(const ColumnsDescription & columns, const ContextPtr & context)
{
    /// Most tables have no MATERIALIZED columns at all, and the lists below are rebuilt on every call.
    if (std::ranges::none_of(columns, isMaterializedColumn))
        return;

    /// `getAllPhysical` omits EPHEMERAL columns, but a MATERIALIZED expression may read one and
    /// analysing it without them fails with UNKNOWN_IDENTIFIER.
    auto source_columns = columns.getAllPhysical();
    NameSet ephemeral_columns;
    for (const auto & column : columns.getEphemeral())
    {
        ephemeral_columns.insert(column.name);
        source_columns.push_back(column);
    }

    for (const auto & column : columns)
    {
        if (!isMaterializedColumn(column))
            continue;

        Column materialized;
        materialized.expression = column.default_desc.expression->clone();
        /// Both the read set and the recompute stage are keyed on top-level columns, so a default
        /// over a subcolumn must be reported as depending on `t`, not on `t.a`.
        replaceSubcolumnsToGetSubcolumnFunctionInQuery(materialized.expression, source_columns);

        /// `analyze` rewrites in place, so keep `materialized.expression` pristine for callers.
        auto query = materialized.expression->clone();
        materialized.dependencies = TreeRewriter(context).analyze(query, source_columns)->requiredSourceColumns();
        materialized.reads_ephemeral = std::ranges::any_of(
            materialized.dependencies, [&](const auto & dependency) { return ephemeral_columns.contains(dependency); });

        materialized_columns.emplace(column.name, std::move(materialized));
    }
}

const MaterializedColumnDependencies::Column * MaterializedColumnDependencies::tryGet(const String & column_name) const
{
    auto it = materialized_columns.find(column_name);
    return it == materialized_columns.end() ? nullptr : &it->second;
}

NameSet MaterializedColumnDependencies::getAffected(const NameSet & changed_columns) const
{
    NameSet affected;
    if (changed_columns.empty())
        return affected;

    NameSet reachable = changed_columns;
    bool changed = true;
    while (changed)
    {
        changed = false;
        for (const auto & [column_name, materialized] : materialized_columns)
        {
            if (materialized.reads_ephemeral || affected.contains(column_name))
                continue;

            if (std::ranges::any_of(materialized.dependencies, [&](const auto & dep) { return reachable.contains(dep); }))
            {
                affected.insert(column_name);
                reachable.insert(column_name);
                changed = true;
            }
        }
    }

    return affected;
}

}
