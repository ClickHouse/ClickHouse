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

    /// Compile the statement, retrying while the database is locked by another connection. Done lazily on the
    /// first `generate` (rather than in the constructor) so it runs under the executor and stays cancellable:
    /// `sqlite3_prepare_v2` of a `SELECT` needs a shared lock to read `sqlite_master`, so under a concurrent
    /// exclusive lock it would otherwise fail outright before the `sqlite3_step` retry loop is ever reached.
    /// Leaves `compiled_statement` null if the read is cancelled while waiting for the lock.
    void prepareStatement();

    String query_str;
    UInt64 max_block_size;

    SQLiteStatementReader statement_reader;
    SQLitePtr sqlite_db;
    std::unique_ptr<sqlite3_stmt, StatementDeleter> compiled_statement;
    bool prepared = false;
};

}

#endif
