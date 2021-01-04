#include <Interpreters/WhereConstraintsOptimizer.h>

#include <Interpreters/ConstraintMatcherVisitor.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

ConstraintMatcherVisitor::Data getConstraintData(const StorageMetadataPtr & metadata_snapshot)
{
    ConstraintMatcherVisitor::Data constraint_data;

    for (const auto & constraint : metadata_snapshot->getConstraints().filterConstraints(ConstraintsDescription::ConstraintType::ALWAYS_TRUE))
    {
        const auto expr = constraint->as<ASTConstraintDeclaration>()->expr->clone();

        constraint_data.constraints[expr->getTreeHash().second].push_back(expr);
    }

    return constraint_data;
}

WhereConstraintsOptimizer::WhereConstraintsOptimizer(
    ASTSelectQuery * select_query_,
    Aliases & /*aliases_*/,
    const NameSet & /*source_columns_set_*/,
    const std::vector<TableWithColumnNamesAndTypes> & /*tables_with_columns_*/,
    const StorageMetadataPtr & metadata_snapshot_)
    : select_query(select_query_)
   /* , aliases(aliases_)
    , source_columns_set(source_columns_set_)
    , tables_with_columns(tables_with_columns_)*/
    , metadata_snapshot(metadata_snapshot_)
{
}

void WhereConstraintsOptimizer::perform()
{
    auto constraint_data = getConstraintData(metadata_snapshot);

    /// TODO: split != -> NOT =, >= -> NOT <, ...

    if (select_query->where())
        ConstraintMatcherVisitor(constraint_data).visit(select_query->refWhere());
}

}
