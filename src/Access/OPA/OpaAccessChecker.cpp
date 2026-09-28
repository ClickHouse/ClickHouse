#include <Access/OPA/OpaAccessChecker.h>

#include <Access/Common/AccessRightsElement.h>


namespace DB
{

OpaAccessChecker::OpaAccessChecker(OpaConfigurationPtr configuration_)
    : configuration(std::move(configuration_))
    , client(configuration)
{
}

bool OpaAccessChecker::governs(const String & user_name, const AccessRightsElement & element) const
{
    /// An administrative or technical account has to keep working while a policy is broken, which is
    /// the only way to repair the configuration that broke it.
    if (configuration->isUserExempt(user_name))
        return false;

    /// A check that names no database is a global privilege - `SYSTEM SHUTDOWN`, introspection,
    /// access management. Those are not objects a data policy describes, and they stay with native
    /// grants; a policy that wanted to restrict them could not name them anyway.
    if (element.anyDatabase())
        return false;

    /// A global privilege with a parameter, such as a named collection or a table engine, carries the
    /// parameter in the database slot, so it would otherwise look like a database-scoped check.
    if (element.isGlobalWithParameter())
        return false;

    return configuration->isDatabaseInScope(element.database);
}

bool OpaAccessChecker::isAllowed(const AccessRightsElement & element, const OpaRequestContext & request_context) const
{
    OpaRequest request;

    for (const auto & keyword : element.access_flags.toKeywords())
        request.operations.emplace_back(keyword);

    if (element.anyTable())
    {
        request.resource = OpaResource::forDatabase(element.database);
    }
    else
    {
        /// `anyColumn` means the check is about the table as a whole, so no column list is sent and a
        /// policy can tell that case apart from a check that names columns.
        Names columns;
        if (!element.anyColumn())
            columns = element.columns;

        request.resource = OpaResource::forTable(element.database, element.table, std::move(columns));
    }

    return client.isAllowed(request, request_context);
}

}
