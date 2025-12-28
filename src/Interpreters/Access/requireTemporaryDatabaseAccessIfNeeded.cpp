#include <Interpreters/Access/requireTemporaryDatabaseAccessIfNeeded.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>

namespace DB
{

bool requireTemporaryDatabaseAccessIfNeeded(AccessRightsElements & required_access, const String & db_name, const ContextPtr & context)
{
    if (db_name.empty())
        return false;

    const auto db = DatabaseCatalog::instance().tryGetDatabase(db_name, context);
    if (db && db->isTemporary())
    {
        // todo: duplicates in result?
        // we could skip check here at all because access already granted if the temporary database presents in context, but permissions may be changed since creation.
        required_access.emplace_back(AccessType::CREATE_TEMPORARY_DATABASE, db_name);
        return true;
    }

    return false;
}

}
