#pragma once

#include "config_core.h"

#if USE_SQLITE
#include <Core/ExternalResultDescription.h>
#include <Processors/Sources/SourceWithProgress.h>

#include <sqlite3.h>


namespace DB
{

class SQLiteSource : public SourceWithProgress
{

using SQLitePtr = std::shared_ptr<sqlite3>;

public:
    SQLiteSource(SQLitePtr sqlite_db_, const String & query_str_, const Block & sample_block, UInt64 max_block_size_);

    String getName() const override { return "SQLite"; }

private:

    using ValueType = ExternalResultDescription::ValueType;

    struct StatementDeleter
    {
        void operator()(sqlite3_stmt * stmt) { sqlite3_finalize(stmt); }
    };

    Chunk generate() override;

    void insertValue(IColumn & column, ExternalResultDescription::ValueType type, size_t idx);

    String query_str;
    UInt64 max_block_size;

    ExternalResultDescription description;
    SQLitePtr sqlite_db;
    std::unique_ptr<sqlite3_stmt, StatementDeleter> compiled_statement;
};

}

#endif
