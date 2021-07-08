#include "SQLiteBlockInputStream.h"

#if USE_SQLITE

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <common/range.h>

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
        {
            compiled_statement.reset();
            break;
        }
        else if (status != SQLITE_ROW)
            throw Exception(status, sqlite3_errstr(status), sqlite3_errmsg(connection.get()));

        int column_count = sqlite3_column_count(compiled_statement.get());

        for (const auto idx : collections::range(0, column_count))
        {
            if (description.types[idx].second)
            {
                ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                insertValue(column_nullable.getNestedColumn(), description.types[idx].first, idx);
                column_nullable.getNullMapData().emplace_back(0);
            }
            else
            {
                insertValue(*columns[idx], description.types[idx].first, idx);
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
        compiled_statement.reset();
}

void SQLiteBlockInputStream::insertValue(
    IColumn & column, const ExternalResultDescription::ValueType type, size_t idx)
{
    switch (type)
    {
        case ValueType::vtUInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(sqlite3_column_int(compiled_statement.get(), idx));
            break;
        case ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(sqlite3_column_int(compiled_statement.get(), idx));
            break;
        case ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(sqlite3_column_int64(compiled_statement.get(), idx));
            break;
        case ValueType::vtUInt64:
            /// There is no uint64 in sqlite3, only int and int64
            assert_cast<ColumnUInt64 &>(column).insertValue(sqlite3_column_int64(compiled_statement.get(), idx));
            break;
        case ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(sqlite3_column_int(compiled_statement.get(), idx));
            break;
        case ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(sqlite3_column_int(compiled_statement.get(), idx));
            break;
        case ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(sqlite3_column_int(compiled_statement.get(), idx));
            break;
        case ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(sqlite3_column_int(compiled_statement.get(), idx));
            break;
        case ValueType::vtFloat32:
            assert_cast<ColumnFloat32 &>(column).insertValue(sqlite3_column_double(compiled_statement.get(), idx));
            break;
        case ValueType::vtFloat64:
            assert_cast<ColumnFloat64 &>(column).insertValue(sqlite3_column_double(compiled_statement.get(), idx));
            break;
        default:
            const char * data = reinterpret_cast<const char *>(sqlite3_column_text(compiled_statement.get(), idx));
            int len = sqlite3_column_bytes(compiled_statement.get(), idx);
            assert_cast<ColumnString &>(column).insertData(data, len);
            break;
    }
}

}

#endif
