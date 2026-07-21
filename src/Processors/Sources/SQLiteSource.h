#pragma once

#include "config.h"

#if USE_SQLITE
#include <Processors/ISource.h>
#include <Processors/Sources/SQLiteStatementReader.h>

#include <sqlite3.h>


namespace DB
{

class SQLiteSource final : public ISource
{

using SQLitePtr = std::shared_ptr<sqlite3>;

public:
    /// The connection must be dedicated to this source: cancellation aborts the running statement with
    /// `sqlite3_interrupt`, which is connection-wide in SQLite, so a handle shared with other queries
    /// would let cancelling this source interrupt an unrelated sibling statement.
    SQLiteSource(SQLitePtr sqlite_db_, const String & query_str_, const Block & sample_block, UInt64 max_block_size_);

    String getName() const override { return "SQLite"; }

private:
    struct StatementDeleter
    {
        void operator()(sqlite3_stmt * stmt) { sqlite3_finalize(stmt); }
    };

    Chunk generate() override;

    void onCancel() noexcept override;

    String query_str;
    UInt64 max_block_size;

    SQLiteStatementReader statement_reader;
    SQLitePtr sqlite_db;
    std::unique_ptr<sqlite3_stmt, StatementDeleter> compiled_statement;
};

}

#endif
