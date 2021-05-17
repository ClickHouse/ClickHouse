#include "SQLiteBlockInputStream.h"

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <ext/range.h>

#include <common/logger_useful.h>

namespace DB
{
SQLiteBlockInputStream::SQLiteBlockInputStream(
    std::shared_ptr<sqlite3> connection_, const std::string & query_str_, const Block & sample_block, const UInt64 max_block_size_)
    : query_str(query_str_), max_block_size(max_block_size_), connection(std::move(connection_))
{
    description.init(sample_block);
}

void SQLiteBlockInputStream::readPrefix()
{
    sqlite3_stmt * compiled_stmt = nullptr;
    int status = sqlite3_prepare_v2(connection.get(), query_str.c_str(), query_str.size() + 1, &compiled_stmt, nullptr);

    if (status != SQLITE_OK)
    {
        throw Exception(status, sqlite3_errstr(status));
    }

    compiled_statement = std::unique_ptr<sqlite3_stmt, StatementDeleter>(compiled_stmt, StatementDeleter());
}

Block SQLiteBlockInputStream::readImpl()
{
    if (!compiled_statement)
        return Block();

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (true)
    {
        int status = sqlite3_step(compiled_statement.get());

        if (status == SQLITE_BUSY)
            continue;
        else if (status == SQLITE_DONE)
            break;
        else if (status != SQLITE_ROW)
            throw Exception(status, sqlite3_errstr(status), sqlite3_errmsg(connection.get()));

        int column_count = sqlite3_column_count(compiled_statement.get());

        for (const auto idx : ext::range(0, column_count))
        {
            const auto & sample = description.sample_block.getByPosition(idx);

            int column_type = sqlite3_column_type(compiled_statement.get(), idx);

            Poco::Logger * log = &(Poco::Logger::get("SQLiteBlockInputStream"));

            switch (column_type)
            {
                case SQLITE_INTEGER:
                    assert_cast<ColumnInt64 &>(*columns[idx]).insertValue(sqlite3_column_int64(compiled_statement.get(), idx));
                    break;
                case SQLITE_FLOAT:
                    assert_cast<ColumnFloat64 &>(*columns[idx]).insertValue(sqlite3_column_double(compiled_statement.get(), idx));
                    break;
                case SQLITE3_TEXT:
                    [[fallthrough]];
                case SQLITE_BLOB:
                    [[fallthrough]];
                case SQLITE_NULL:
                {
                    const char * data = reinterpret_cast<const char *>(sqlite3_column_text(compiled_statement.get(), idx));
                    int len = sqlite3_column_bytes(compiled_statement.get(), idx);
                    LOG_INFO(
                        log,
                        "GOT DATA on col = {}, dest type = {}, with len={} : {}",
                        idx,
                        sample.type->getName(),
                        len,
                        (data ? data : "NO"));
                    //                    if (!data) {
                    //                        (*columns[idx]).insertFrom(*sample.column, 0); break;
                    //                    }
                    assert_cast<ColumnString &>(*columns[idx]).insertData(data, len);
                    break;
                }
                    //                {
                    //                    LOG_INFO(log, "GOT DATA on col = {}, dest type = {}, NULL", idx, sample.type->getName());
                    //                    (*columns[idx]).insertFrom(*sample.column, 0); break;
                    //                }
            }
        }

        if (++num_rows == max_block_size)
            break;
    }

    return description.sample_block.cloneWithColumns(std::move(columns));
}
void SQLiteBlockInputStream::readSuffix()
{
    if (compiled_statement)
    {
        compiled_statement.reset();
    }
}

}
