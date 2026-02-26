#include "getTableOverride.h"

#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTTableOverrides.h>
#include <Databases/IDatabase.h>

namespace DB
{

ASTPtr tryGetTableOverride(const String & mapped_database, const String & table)
{
    if (auto database_ptr = DatabaseCatalog::instance().tryGetDatabase(mapped_database))
    {
        auto create_query = database_ptr->getCreateDatabaseQuery();
        if (auto * create_database_query = create_query->as<ASTCreateQuery>())
        {
            if (create_database_query->table_overrides)
            {
                return create_database_query->table_overrides->tryGetTableOverride(table);
            }
        }
    }
    return nullptr;
}

}
