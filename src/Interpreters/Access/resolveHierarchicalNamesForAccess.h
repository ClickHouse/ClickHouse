#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>


namespace DB
{

class AccessRightsElements;
class ASTRowPolicyNames;

/// An exact object name of an access statement (`GRANT SELECT ON a.b.c`, `CREATE ROW POLICY p ON a.b.c`) is a
/// hierarchical name, the same as in a query: it has no fixed split into a database and a table, and it is resolved
/// against the catalog, so that a privilege or a policy names the object that the same name reads in a `SELECT`.
/// The names of whole databases and the wildcards are left as written: they are prefixes, not object names.
void resolveHierarchicalNameForAccess(String & database, String & table, ContextPtr context);

void resolveHierarchicalNamesForAccess(AccessRightsElements & elements, ContextPtr context);
void resolveHierarchicalNamesForAccess(ASTRowPolicyNames & names, ContextPtr context);

}
