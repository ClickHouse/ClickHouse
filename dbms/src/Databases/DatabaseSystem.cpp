#include <DB/Databases/DatabaseSystem.h>


namespace DB
{

DatabaseSystem::DatabaseSystem()
{

}

bool DatabaseSystem::isTableExist(const String & name) const;

StoragePtr DatabaseSystem::tryGetTable(const String & name);

DatabaseIteratorPtr DatabaseSystem::getIterator();

void DatabaseSystem::addTable(const String & name, StoragePtr & table, const ASTPtr & query, const String & engine);

StoragePtr DatabaseSystem::detachTable(const String & name, bool remove_metadata);

ASTPtr DatabaseSystem::getCreateQuery(const String & name) const;

}
