#include <Interpreters/Access/resolveHierarchicalNamesForAccess.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/Access/ASTRowPolicyName.h>


namespace DB
{

void resolveHierarchicalNameForAccess(String & database, String & table, ContextPtr context)
{
    /// A name without a database, or a name of a whole database, is not an object name to resolve.
    if (database.empty() || table.empty())
        return;

    /// Only a name with dots has other candidates.
    if (!database.contains('.') && !table.contains('.'))
        return;

    StorageID resolved = DatabaseCatalog::instance().resolveHierarchicalName(StorageID(database, table), context);
    database = resolved.database_name;
    table = resolved.table_name;
}

void resolveHierarchicalNamesForAccess(AccessRightsElements & elements, ContextPtr context)
{
    for (auto & element : elements)
    {
        /// A wildcard (`GRANT SELECT ON db.tab*`) matches a prefix of a name, and a parameter is not a table.
        if (element.wildcard || !element.parameter.empty())
            continue;
        resolveHierarchicalNameForAccess(element.database, element.table, context);
    }
}

void resolveHierarchicalNamesForAccess(ASTRowPolicyNames & names, ContextPtr context)
{
    for (auto & full_name : names.full_names)
    {
        if (full_name.table_name == RowPolicyName::ANY_TABLE_MARK)
            continue;
        resolveHierarchicalNameForAccess(full_name.database, full_name.table_name, context);
    }
}

}
