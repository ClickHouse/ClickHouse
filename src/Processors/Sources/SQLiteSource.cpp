#include "SQLiteSource.h"

#if USE_SQLITE
#include <base/range.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeNullable.h>


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
    : SourceWithProgress(sample_block.cloneEmpty())
    , query_str(query_str_)
    , max_block_size(max_block_size_)
    , sqlite_db(std::move(sqlite_db_))
{
    description.init(sample_block);

    sqlite3_stmt * compiled_stmt = nullptr;
    int status = sqlite3_prepare_v2(sqlite_db.get(), query_str.c_str(), query_str.size() + 1, &compiled_stmt, nullptr);

    if (status != SQLITE_OK)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Cannot prepate sqlite statement. Status: {}. Message: {}",
                        status, sqlite3_errstr(status));

    compiled_statement = std::unique_ptr<sqlite3_stmt, StatementDeleter>(compiled_stmt, StatementDeleter());
}

Chunk SQLiteSource::generate()
{
    if (!compiled_statement)
        return {};

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (true)
    {
        int status = sqlite3_step(compiled_statement.get());

        if (status == SQLITE_BUSY)
        {
            continue;
        }
        else if (status == SQLITE_DONE)
        {
            compiled_statement.reset();
            break;
        }
        else if (status != SQLITE_ROW)
        {
            throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                "Expected SQLITE_ROW status, but got status {}. Error: {}, Message: {}",
                status, sqlite3_errstr(status), sqlite3_errmsg(sqlite_db.get()));
        }

        int column_count = sqlite3_column_count(compiled_statement.get());

        for (int column_index = 0; column_index < column_count; ++column_index)
        {
            if (sqlite3_column_type(compiled_statement.get(), column_index) == SQLITE_NULL)
            {
                columns[column_index]->insertDefault();
                continue;
            }

            auto & [type, is_nullable] = description.types[column_index];
            if (is_nullable)
            {
                ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[column_index]);
                insertValue(column_nullable.getNestedColumn(), type, column_index);
                column_nullable.getNullMapData().emplace_back(0);
            }
            else
            {
                insertValue(*columns[column_index], type, column_index);
            }
        }

        if (++num_rows == max_block_size)
            break;
    }

    if (num_rows == 0)
    {
        compiled_statement.reset();
        return {};
    }

    return Chunk(std::move(columns), num_rows);
}

void SQLiteSource::insertValue(IColumn & column, ExternalResultDescription::ValueType type, size_t idx)
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
            assert_cast<ColumnInt64 &>(column).insertValue(sqlite3_column_int64(compiled_statement.get(), idx));
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
