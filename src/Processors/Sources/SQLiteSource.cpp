#include <Processors/Sources/SQLiteSource.h>

#if USE_SQLITE
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SQLITE_ENGINE_ERROR;
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
    sqlite3_stmt * compiled_stmt = nullptr;
    int status = sqlite3_prepare_v2(
        sqlite_db.get(),
        query_str.c_str(),
        static_cast<int>(query_str.size() + 1),
        &compiled_stmt, nullptr);

    if (status != SQLITE_OK)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Cannot prepare sqlite statement. Status: {}. Message: {}",
                        status, sqlite3_errstr(status));

    compiled_statement = std::unique_ptr<sqlite3_stmt, StatementDeleter>(compiled_stmt, StatementDeleter());
}

Chunk SQLiteSource::generate()
{
    LOG_TEST(getLogger("SQLiteSource"), "Generate a chunk");

    if (!compiled_statement)
        return {};

    bool finished = false;
    auto chunk = statement_reader.readChunk(sqlite_db.get(), compiled_statement.get(), max_block_size, finished);
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
