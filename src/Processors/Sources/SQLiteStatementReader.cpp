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
#include <DataTypes/DataTypeLowCardinality.h>
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
    extern const int SQLITE_ENGINE_ERROR;
}

namespace
{

std::optional<ExternalResultDescription::ValueType> getValueTypeForDirectRead(const IDataType & type)
{
    WhichDataType which(type);

    using ValueType = ExternalResultDescription::ValueType;

    if (which.isUInt8())
        return ValueType::vtUInt8;
    if (which.isUInt16())
        return ValueType::vtUInt16;
    if (which.isUInt32())
        return ValueType::vtUInt32;
    if (which.isUInt64())
        return ValueType::vtUInt64;
    if (which.isInt8())
        return ValueType::vtInt8;
    if (which.isInt16())
        return ValueType::vtInt16;
    if (which.isInt32())
        return ValueType::vtInt32;
    if (which.isInt64())
        return ValueType::vtInt64;
    if (which.isFloat32())
        return ValueType::vtFloat32;
    if (which.isFloat64())
        return ValueType::vtFloat64;
    if (which.isEnum8())
        return ValueType::vtEnum8;
    if (which.isEnum16())
        return ValueType::vtEnum16;
    if (which.isString())
        return ValueType::vtString;
    if (which.isDate())
        return ValueType::vtDate;
    if (which.isDate32())
        return ValueType::vtDate32;
    if (which.isDateTime())
        return ValueType::vtDateTime;
    if (which.isUUID())
        return ValueType::vtUUID;
    if (which.isFixedString())
        return ValueType::vtFixedString;

    return std::nullopt;
}

std::string_view getTextValue(sqlite3_stmt * statement, int idx)
{
    const char * data = reinterpret_cast<const char *>(sqlite3_column_text(statement, idx));
    int len = sqlite3_column_bytes(statement, idx);
    if (!data && len)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot read text value from SQLite database");

    return {data ? data : "", static_cast<size_t>(len)};
}

}

SQLiteStatementReader::ColumnReadInfo SQLiteStatementReader::createColumnReadInfo(const ColumnWithTypeAndName & column) const
{
    ColumnReadInfo info;
    info.serialization = column.type->getDefaultSerialization();
    info.is_nullable = column.type->isNullable();

    DataTypePtr type_for_direct_read = removeNullable(column.type);
    info.data_type = type_for_direct_read;

    if (value_read_mode == ValueReadMode::Native
        && !WhichDataType(column.type).isLowCardinality()
        && !WhichDataType(type_for_direct_read).isLowCardinality())
        info.native_value_type = getValueTypeForDirectRead(*type_for_direct_read);

    return info;
}

SQLiteStatementReader::SQLiteStatementReader(
    const Block & sample_block_,
    const FormatSettings & format_settings_,
    ValueReadMode value_read_mode_)
    : sample_block(sample_block_.cloneEmpty())
    , format_settings(format_settings_)
    , value_read_mode(value_read_mode_)
{
    columns_info.reserve(sample_block.columns());

    for (const auto & column : sample_block)
        columns_info.push_back(createColumnReadInfo(column));
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
            continue;

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
            if (sqlite3_column_type(statement, column_index) == SQLITE_NULL)
            {
                columns[column_index]->insertDefault();
                continue;
            }

            const auto & info = columns_info[column_index];
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
            assert_cast<ColumnFloat32 &>(column).insertValue(static_cast<Float32>(sqlite3_column_double(statement, idx)));
            break;
        case ValueType::vtFloat64:
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
