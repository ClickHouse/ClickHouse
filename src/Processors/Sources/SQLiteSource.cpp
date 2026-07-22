#include <Processors/Sources/SQLiteSource.h>

#if USE_SQLITE
#include <base/sleep.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SQLITE_ENGINE_ERROR;
}

namespace
{
    /// How long to sleep between `sqlite3_prepare_v2` retries when the database is locked (SQLITE_BUSY),
    /// matching the `sqlite3_step` retry back-off in `SQLiteStatementReader`.
    constexpr UInt64 sqlite_busy_retry_ms = 10;
}

SQLiteSource::SQLiteSource(
    SQLitePtr sqlite_db_,
    const String & query_str_,
    const Block & sample_block,
    const UInt64 max_block_size_)
    : ISource(std::make_shared<const Block>(sample_block.cloneEmpty()))
    , query_str(query_str_)
    , max_block_size(max_block_size_)
    , statement_reader(sample_block, FormatSettings{}, SQLiteStatementReader::ValueReadMode::Native)
    , sqlite_db(std::move(sqlite_db_))
{
}

void SQLiteSource::prepareStatement()
{
    while (true)
    {
        sqlite3_stmt * compiled_stmt = nullptr;
        int status = sqlite3_prepare_v2(
            sqlite_db.get(),
            query_str.c_str(),
            static_cast<int>(query_str.size() + 1),
            &compiled_stmt, nullptr);

        if (status == SQLITE_OK)
        {
            compiled_statement = std::unique_ptr<sqlite3_stmt, StatementDeleter>(compiled_stmt, StatementDeleter());
            return;
        }

        /// The database is locked by another connection. Idle and stay cancellable instead of busy-spinning or
        /// failing outright, mirroring the `sqlite3_step` retry loop in `SQLiteStatementReader::readChunk`.
        /// `sqlite3_interrupt` (our cancellation path) makes an in-progress prepare return `SQLITE_INTERRUPT`.
        if (status == SQLITE_BUSY || status == SQLITE_INTERRUPT)
        {
            if (isCancelled())
                return;
            if (status == SQLITE_BUSY)
                sleepForMilliseconds(sqlite_busy_retry_ms);
            continue;
        }

        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Cannot prepare sqlite statement. Status: {}. Message: {}",
                        status, sqlite_db ? sqlite3_errmsg(sqlite_db.get()) : sqlite3_errstr(status));
    }
}

Chunk SQLiteSource::generate()
{
    if (!prepared)
    {
        prepared = true;
        prepareStatement();
    }

    LOG_TEST(getLogger("SQLiteSource"), "Generate a chunk");

    if (!compiled_statement)
        return {};

    bool finished = false;
    auto chunk = statement_reader.readChunk(
        sqlite_db.get(), compiled_statement.get(), max_block_size, finished, [this] { return isCancelled(); });
    if (finished)
        compiled_statement.reset();

    return chunk;
}

void SQLiteSource::onCancel() noexcept
{
    try
    {
        if (sqlite_db)
        {
            sqlite3_interrupt(sqlite_db.get());
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}

#endif
