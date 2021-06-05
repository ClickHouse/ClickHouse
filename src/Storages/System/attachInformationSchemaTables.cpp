#include <Databases/IDatabase.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/System/attachSystemTablesImpl.h>

#include <Storages/System/StorageSystemTablesIS.h>
#include <Storages/System/StorageSystemColumnsIS.h>
#include <Storages/System/StorageSystemViewsIS.h>

namespace DB
{

void attachInformationSchemaLocal(IDatabase & information_schema_database)
{
    attachInformationSchemaTable<StorageSystemTablesIS>(information_schema_database, "tables");
    attachInformationSchemaTable<StorageSystemColumnsIS>(information_schema_database, "columns");
    attachInformationSchemaTable<StorageSystemViewsIS>(information_schema_database, "views");
}

}
