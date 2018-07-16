#pragma once

#include <Storages/ColumnDefault.h>

namespace DB
{
    class Context;
    class Block;

    /// Additional information for query that could not be get from sample block
    struct TableMetadata
    {
        TableMetadata(const String & database_, const String & table_)
            : database(database_), table(table_)
        {}

        const String & database;
        const String & table;
        ColumnDefaults column_defaults;

        bool loadFromContext(const Context & context);
        bool hasDefaults() const { return !column_defaults.empty(); }
    };
}
