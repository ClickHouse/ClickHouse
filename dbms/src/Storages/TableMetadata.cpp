#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include "TableMetadata.h"


namespace DB
{
    void TableMetadata::loadFromContext(const Context & context)
    {
        if (!context.isTableExist(database, table))
            return;

        StoragePtr storage = context.getTable(database, table);
        const ColumnsDescription & table_columns = storage->getColumns();
        column_defaults = table_columns.defaults;
    }
}
