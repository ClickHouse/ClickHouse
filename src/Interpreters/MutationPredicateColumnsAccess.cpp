#include <Interpreters/MutationPredicateColumnsAccess.h>

#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/IAST.h>

namespace DB
{

void addPredicateColumnsSelectAccess(
    AccessRightsElements & required_access, const IAST * predicate, const String & database, const String & table)
{
    if (!predicate)
        return;

    RequiredSourceColumnsVisitor::Data columns_context;
    auto predicate_clone = predicate->clone();
    RequiredSourceColumnsVisitor(columns_context).visit(predicate_clone);

    Strings columns;
    const String db_table_prefix = database.empty() ? String{} : database + "." + table + ".";
    const String table_prefix = table + ".";
    for (const auto & name : columns_context.requiredColumns())
    {
        if (!db_table_prefix.empty() && name.starts_with(db_table_prefix))
            columns.emplace_back(name.substr(db_table_prefix.size()));
        else if (name.starts_with(table_prefix))
            columns.emplace_back(name.substr(table_prefix.size()));
        else
            columns.emplace_back(name);
    }

    if (!columns.empty())
        required_access.emplace_back(AccessType::SELECT, database, table, columns);
}

}
