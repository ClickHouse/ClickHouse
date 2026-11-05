#include <Storages/MergeTree/projectionPartCanFillDefault.h>

#include <Common/checkStackSize.h>
#include <Common/scope_guard_safe.h>
#include <Core/Names.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

namespace
{

bool canFillDefault(
    const IMergeTreeDataPart & projection_part,
    const IMergeTreeDataPart & parent_part,
    const ProjectionDescription & projection,
    const ColumnsDescription & parent_table_columns,
    const String & column_name,
    NameSet & resolving,
    NameSet & known_fillable)
{
    /// Incremental ALTERs can chain defaults arbitrarily deep.
    checkStackSize();

    if (known_fillable.contains(column_name))
        return true;

    /// Only a name already on the current resolution path is a cycle, so it is erased on the way out:
    /// two branches sharing a dependency (`d DEFAULT e + f`, both reading `g`) are not a cycle.
    if (!resolving.emplace(column_name).second)
        return false;
    SCOPE_EXIT({ resolving.erase(column_name); });

    /// A subcolumn has no default of its own: it is extracted from the default of its column in storage.
    auto column_default = parent_table_columns.getDefault(column_name);
    if (!column_default)
    {
        auto column_in_storage = parent_table_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name);
        if (column_in_storage && column_in_storage->isSubcolumn())
            column_default = parent_table_columns.getDefault(column_in_storage->getNameInStorage());
    }

    if (!column_default || !column_default->expression)
    {
        known_fillable.insert(column_name);
        return true;
    }

    const auto & projection_columns = projection.metadata->getColumns();

    /// Masks lambda formals, as the reader does when it synthesizes a default.
    RequiredSourceColumnsVisitor::Data columns_context;
    RequiredSourceColumnsVisitor(columns_context).visit(column_default->expression);

    for (const auto & identifier : columns_context.requiredColumns())
    {
        if ((projection_part.tryGetColumn(identifier)
             && projection_columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, identifier))
            || projection.metadata->virtuals.has(identifier))
            continue;

        /// Not a column: a dictionary or `Join` table named by a bare identifier (`dictGet(dict, ...)`),
        /// which the reader skips as well.
        if (!parent_table_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, identifier))
            continue;

        /// Otherwise fillable only as a late-add: a stored table column the parent part lacks too, still
        /// required by the projection, whose own default is fillable. A column the parent part stores
        /// would be filled here with a value that differs from the parent read.
        if (parent_part.tryGetColumn(identifier)
            || !projection_columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, identifier)
            || !canFillDefault(projection_part, parent_part, projection, parent_table_columns, identifier, resolving, known_fillable))
            return false;
    }

    known_fillable.insert(column_name);
    return true;
}

}

bool projectionPartCanFillDefault(
    const IMergeTreeDataPart & projection_part,
    const IMergeTreeDataPart & parent_part,
    const ProjectionDescription & projection,
    const ColumnsDescription & parent_table_columns,
    const String & column_name)
{
    NameSet resolving;
    NameSet known_fillable;
    return canFillDefault(projection_part, parent_part, projection, parent_table_columns, column_name, resolving, known_fillable);
}

}
