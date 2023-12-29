#include <Databases/DatabaseFactory.h>
#include <Databases/registerDatabases.h>


namespace DB
{

void registerDatabaseAtomic(DatabaseFactory & factory);
void registerDatabaseOrdinary(DatabaseFactory & factory);
void registerDatabaseDictionary(DatabaseFactory & factory);
void registerDatabaseMemory(DatabaseFactory & factory);
void registerDatabaseLazy(DatabaseFactory & factory);
void registerDatabaseFilesystem(DatabaseFactory & factory);


void registerDatabases()
{
    auto & factory = DatabaseFactory::instance();
    registerDatabaseAtomic(factory);
    registerDatabaseOrdinary(factory);
    registerDatabaseDictionary(factory);
    registerDatabaseMemory(factory);
    registerDatabaseLazy(factory);
    registerDatabaseFilesystem(factory);
}
}
