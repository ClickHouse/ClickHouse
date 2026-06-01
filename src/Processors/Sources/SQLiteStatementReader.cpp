#include <Processors/Sources/SQLiteStatementReader.h>

#if USE_SQLITE

#include <Common/assert_cast.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
    extern const int SQLITE_ENGINE_ERROR;
}

namespace
{

std::string_view getTextValue(sqlite3_stmt * statement, int idx)
{
    const char * data = reinterpret_cast<const char *>(sqlite3_column_text(statement, idx));
    int len = sqlite3_column_bytes(statement, idx);
    if (!data && len)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot read text value from SQLite database");

    return {data ? data : "", static_cast<size_t>(len)};
}

}

SQLiteStatementReader::ColumnReadInfo SQLiteStatementReader::createColumnReadInfoForNative(
    const ColumnWithTypeAndName & column,
    ValueType native_value_type,
    bool is_nullable) const
{
    ColumnReadInfo info;
    info.name = column.name;
    auto type_not_nullable = removeNullable(column.type);
    info.serialization = type_not_nullable->getDefaultSerialization();
    info.is_nullable = is_nullable;
    info.data_type = type_not_nullable;
    info.native_value_type = native_value_type;

    return info;
}

SQLiteStatementReader::ColumnReadInfo SQLiteStatementReader::createColumnReadInfoForText(const ColumnWithTypeAndName & column) const
{
    ColumnReadInfo info;
    info.name = column.name;
    info.serialization = column.type->getDefaultSerialization();
    info.is_nullable = canContainNull(*column.type);
    info.data_type = removeNullable(column.type);

    return info;
}

SQLiteStatementReader::SQLiteStatementReader(
    const Block & sample_block_,
    const FormatSettings & format_settings_,
    ValueReadMode value_read_mode_)
    : format_settings(format_settings_)
{
    if (value_read_mode_ == ValueReadMode::Native)
    {
        ExternalResultDescription description;
        description.init(sample_block_);

        sample_block = description.sample_block.cloneEmpty();
        columns_info.reserve(description.sample_block.columns());

        for (size_t i = 0; i != description.sample_block.columns(); ++i)
        {
            const auto & column = description.sample_block.getByPosition(i);
            const auto & [value_type, is_nullable] = description.types[i];
            columns_info.push_back(createColumnReadInfoForNative(column, value_type, is_nullable));
        }

        return;
    }

    sample_block = sample_block_.cloneEmpty();
    columns_info.reserve(sample_block.columns());

    for (const auto & column : sample_block)
        columns_info.push_back(createColumnReadInfoForText(column));
}

Chunk SQLiteStatementReader::readChunk(sqlite3 * db, sqlite3_stmt * statement, UInt64 max_block_size, bool & finished)
{
    finished = false;

    MutableColumns columns = sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (num_rows < max_block_size)
    {
        int status = sqlite3_step(statement);

        if (status == SQLITE_INTERRUPT || status == SQLITE_DONE)
        {
            finished = true;
            break;
        }

        if (status == SQLITE_BUSY)
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "SQLite database is busy. Error: {}, Message: {}",
                sqlite3_errstr(status),
                sqlite3_errmsg(db));

        if (status != SQLITE_ROW)
        {
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "Expected SQLITE_ROW status, but got status {}. Error: {}, Message: {}",
                status,
                sqlite3_errstr(status),
                sqlite3_errmsg(db));
        }

        int column_count = sqlite3_column_count(statement);
        if (column_count != static_cast<int>(columns_info.size()))
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "Expected {} columns from SQLite query, but got {}",
                columns_info.size(),
                column_count);

        for (int column_index = 0; column_index != column_count; ++column_index)
        {
            const auto & info = columns_info[column_index];

            if (sqlite3_column_type(statement, column_index) == SQLITE_NULL)
            {
                if (!info.is_nullable && !format_settings.null_as_default)
                    throw Exception(
                        ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN,
                        "Cannot insert NULL value into non-Nullable column {}",
                        info.name);

                columns[column_index]->insertDefault();
                continue;
            }

            if (info.is_nullable && info.native_value_type)
            {
                auto & column_nullable = assert_cast<ColumnNullable &>(*columns[column_index]);
                insertValue(column_nullable.getNestedColumn(), info, statement, column_index);
                column_nullable.getNullMapData().emplace_back(false);
            }
            else
            {
                insertValue(*columns[column_index], info, statement, column_index);
            }
        }

        ++num_rows;
    }

    return num_rows ? Chunk(std::move(columns), num_rows) : Chunk{};
}

void SQLiteStatementReader::insertValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const
{
    if (!info.native_value_type)
    {
        insertTextValue(column, info, statement, idx);
        return;
    }

    switch (*info.native_value_type)
    {
        case ValueType::vtUInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(static_cast<UInt8>(sqlite3_column_int(statement, idx)));
            break;
        case ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(static_cast<UInt16>(sqlite3_column_int(statement, idx)));
            break;
        case ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(sqlite3_column_int64(statement, idx)));
            break;
        case ValueType::vtUInt64:
            /// There is no uint64 in sqlite3, only int and int64.
            assert_cast<ColumnUInt64 &>(column).insertValue(sqlite3_column_int64(statement, idx));
            break;
        case ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(static_cast<Int8>(sqlite3_column_int(statement, idx)));
            break;
        case ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(static_cast<Int16>(sqlite3_column_int(statement, idx)));
            break;
        case ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(sqlite3_column_int(statement, idx));
            break;
        case ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(sqlite3_column_int64(statement, idx));
            break;
        case ValueType::vtFloat32:
            if (sqlite3_column_type(statement, idx) == SQLITE_TEXT)
            {
                insertTextValue(column, info, statement, idx);
                break;
            }

            assert_cast<ColumnFloat32 &>(column).insertValue(static_cast<Float32>(sqlite3_column_double(statement, idx)));
            break;
        case ValueType::vtFloat64:
            if (sqlite3_column_type(statement, idx) == SQLITE_TEXT)
            {
                insertTextValue(column, info, statement, idx);
                break;
            }

            assert_cast<ColumnFloat64 &>(column).insertValue(sqlite3_column_double(statement, idx));
            break;
        case ValueType::vtEnum8:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnInt8 &>(column).insertValue(
                static_cast<Int8>(assert_cast<const DataTypeEnum<Int8> &>(*info.data_type).castToValue(value).safeGet<Int8>()));
            break;
        }
        case ValueType::vtEnum16:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnInt16 &>(column).insertValue(
                static_cast<Int16>(assert_cast<const DataTypeEnum<Int16> &>(*info.data_type).castToValue(value).safeGet<Int16>()));
            break;
        }
        case ValueType::vtString:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnString &>(column).insertData(value.data(), value.size());
            break;
        }
        case ValueType::vtDate:
        {
            auto value = getTextValue(statement, idx);
            ReadBufferFromString in(value);
            DayNum day;
            readDateText(day, in);
            assert_cast<ColumnUInt16 &>(column).insertValue(day);
            break;
        }
        case ValueType::vtDate32:
        {
            auto value = getTextValue(statement, idx);
            ReadBufferFromString in(value);
            ExtendedDayNum day;
            readDateText(day, in);
            assert_cast<ColumnInt32 &>(column).insertValue(day);
            break;
        }
        case ValueType::vtDateTime:
        {
            auto value = getTextValue(statement, idx);
            ReadBufferFromString in(value);
            time_t time = 0;
            readDateTimeText(time, in, assert_cast<const DataTypeDateTime &>(*info.data_type).getTimeZone());
            time = std::max<time_t>(time, 0);
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(time));
            break;
        }
        case ValueType::vtUUID:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnUUID &>(column).insert(parse<UUID>(value.data(), value.size()));
            break;
        }
        case ValueType::vtFixedString:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnFixedString &>(column).insertData(value.data(), value.size());
            break;
        }
        default:
            insertTextValue(column, info, statement, idx);
            break;
    }
}

void SQLiteStatementReader::insertTextValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const
{
    auto value = getTextValue(statement, idx);
    ReadBufferFromString buffer(value);
    info.serialization->deserializeWholeText(column, buffer, format_settings);
}

}

#endif
