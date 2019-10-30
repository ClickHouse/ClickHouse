#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOnDisk.h>


namespace DB
{

DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, const Context & context_)
    : DatabaseOrdinary(name_, metadata_path_, context_)
{
}

void DatabaseAtomic::renameTable(const Context & context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, TableStructureWriteLockHolder & lock)
{
    //TODO
    DatabaseOnDisk::renameTable<DatabaseAtomic>(*this, context, table_name, to_database, to_table_name, lock);
}


}

