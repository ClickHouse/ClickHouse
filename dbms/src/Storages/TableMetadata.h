#pragma once

#include <Storages/ColumnDefault.h>

namespace DB
{
    class Context;
    class Block;

    /// Addition information for query that could not be get from sample block
    struct TableMetadata
    {
        TableMetadata(const String & database_, const String & table_)
            : database(database_), table(table_)
        {}

        const String & database;
        const String & table;
        ColumnDefaults column_defaults;

        void loadFromContext(const Context & context);
    };
}
