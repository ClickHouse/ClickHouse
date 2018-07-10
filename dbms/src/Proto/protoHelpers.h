#pragma once

#include <Storages/ColumnDefault.h>

namespace DB
{
    class Context;
    class Block;

    struct TableMetaInfo
    {
        TableMetaInfo(const String & database_, const String & table_)
            : database(database_), table(table_)
        {}

        const String & database;
        const String & table;
        ColumnDefaults column_defaults;
    };

    Block storeContextBlock(Context & context);
    void loadTableMetaInfo(const Block & block, TableMetaInfo & table_meta);
}
