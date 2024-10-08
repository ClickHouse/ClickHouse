#include "config.h"

#if USE_MYSQL
#include <vector>

#include <Core/MySQL/MySQLReplication.h>
#include <Core/Settings.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Common/assert_cast.h>
#include <base/range.h>
#include <Common/logger_useful.h>
#include <Processors/Sources/MySQLSource.h>
#include <boost/algorithm/string.hpp>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 external_storage_max_read_bytes;
    extern const SettingsUInt64 external_storage_max_read_rows;
    extern const SettingsUInt64 max_block_size;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

StreamSettings::StreamSettings(const Settings & settings, bool auto_close_, bool fetch_by_name_, size_t max_retry_)
    : max_read_mysql_row_nums(
          (settings[Setting::external_storage_max_read_rows]) ? settings[Setting::external_storage_max_read_rows] : settings[Setting::max_block_size])
    , max_read_mysql_bytes_size(settings[Setting::external_storage_max_read_bytes])
    , auto_close(auto_close_)
    , fetch_by_name(fetch_by_name_)
    , default_num_tries_on_connection_loss(max_retry_)
{
}

MySQLSource::Connection::Connection(
    const mysqlxx::PoolWithFailover::Entry & entry_,
    const std::string & query_str)
    : entry(entry_)
    , query{entry->query(query_str)}
    , result{query.use()}
{
}

/// Used in MaterializedMySQL and in doInvalidateQuery for dictionary source.
MySQLSource::MySQLSource(
    const mysqlxx::PoolWithFailover::Entry & entry,
    const std::string & query_str,
    const Block & sample_block,
    const StreamSettings & settings_)
    : ISource(sample_block.cloneEmpty())
    , log(getLogger("MySQLSource"))
    , connection{std::make_unique<Connection>(entry, query_str)}
    , settings{std::make_unique<StreamSettings>(settings_)}
{
    description.init(sample_block);
    initPositionMappingFromQueryResultStructure();
}

/// For descendant MySQLWithFailoverSource
MySQLSource::MySQLSource(const Block &sample_block_, const StreamSettings & settings_)
    : ISource(sample_block_.cloneEmpty())
    , log(getLogger("MySQLSource"))
    , settings(std::make_unique<StreamSettings>(settings_))
{
    description.init(sample_block_);
}

/// Used by MySQL storage / table function and dictionary source.
MySQLWithFailoverSource::MySQLWithFailoverSource(
    mysqlxx::PoolWithFailoverPtr pool_,
    const std::string & query_str_,
    const Block & sample_block_,
    const StreamSettings & settings_)
    : MySQLSource(sample_block_, settings_)
    , pool(pool_)
    , query_str(query_str_)
{
}

void MySQLWithFailoverSource::onStart()
{
    size_t count_connect_attempts = 0;

    /// For recovering from "Lost connection to MySQL server during query" errors
    while (true)
    {
        try
        {
            connection = std::make_unique<Connection>(pool->get(), query_str);
            break;
        }
        catch (const mysqlxx::ConnectionLost & ecl)  /// There are two retriable failures: CR_SERVER_GONE_ERROR, CR_SERVER_LOST
        {
            LOG_WARNING(log, "Failed connection ({}/{}). Trying to reconnect... (Info: {})", count_connect_attempts, settings->default_num_tries_on_connection_loss, ecl.displayText());

            if (++count_connect_attempts > settings->default_num_tries_on_connection_loss)
            {
                LOG_ERROR(log, "Failed to create connection to MySQL. ({}/{})", count_connect_attempts, settings->default_num_tries_on_connection_loss);
                throw;
            }
        }
        catch (const mysqlxx::BadQuery & e)
        {
            LOG_ERROR(log, "Error processing query '{}': {}", query_str, e.displayText());
            throw;
        }
    }

    initPositionMappingFromQueryResultStructure();
}

Chunk MySQLWithFailoverSource::generate()
{
    if (!is_initialized)
    {
        onStart();
        is_initialized = true;
    }

    return MySQLSource::generate();
}


namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    void insertValue(const IDataType & data_type, IColumn & column, const ValueType type, const mysqlxx::Value & value, size_t & read_bytes_size, enum enum_field_types mysql_type)
    {
        switch (type)
        {
            case ValueType::vtUInt8:
                assert_cast<ColumnUInt8 &>(column).insertValue(value.getUInt());
                read_bytes_size += 1;
                break;
            case ValueType::vtUInt16:
                assert_cast<ColumnUInt16 &>(column).insertValue(value.getUInt());
                read_bytes_size += 2;
                break;
            case ValueType::vtUInt32:
                assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(value.getUInt()));
                read_bytes_size += 4;
                break;
            case ValueType::vtUInt64:
            {
                if (mysql_type == enum_field_types::MYSQL_TYPE_BIT)
                {
                    size_t n = value.size();
                    UInt64 val = 0UL;
                    ReadBufferFromMemory payload(const_cast<char *>(value.data()), n);
                    MySQLReplication::readBigEndianStrict(payload, reinterpret_cast<char *>(&val), n);
                    assert_cast<ColumnUInt64 &>(column).insertValue(val);
                    read_bytes_size += n;
                }
                else
                {
                    assert_cast<ColumnUInt64 &>(column).insertValue(value.getUInt());
                    read_bytes_size += 8;
                }
                break;
            }
            case ValueType::vtInt8:
                assert_cast<ColumnInt8 &>(column).insertValue(value.getInt());
                read_bytes_size += 1;
                break;
            case ValueType::vtInt16:
                assert_cast<ColumnInt16 &>(column).insertValue(value.getInt());
                read_bytes_size += 2;
                break;
            case ValueType::vtInt32:
                assert_cast<ColumnInt32 &>(column).insertValue(static_cast<Int32>(value.getInt()));
                read_bytes_size += 4;
                break;
            case ValueType::vtInt64:
            {
                if (mysql_type == enum_field_types::MYSQL_TYPE_TIME)
                {
                    String time_str(value.data(), value.size());
                    bool negative = time_str.starts_with("-");
                    if (negative) time_str = time_str.substr(1);
                    std::vector<String> hhmmss;
                    boost::split(hhmmss, time_str, [](char c) { return c == ':'; });
                    Int64 v = 0;

                    if (hhmmss.size() == 3)
                        v = static_cast<Int64>((std::stoi(hhmmss[0]) * 3600 + std::stoi(hhmmss[1]) * 60 + std::stold(hhmmss[2])) * 1000000);
                    else
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported value format");

                    if (negative) v = -v;
                    assert_cast<ColumnInt64 &>(column).insertValue(v);
                    read_bytes_size += value.size();
                }
                else
                {
                    assert_cast<ColumnInt64 &>(column).insertValue(value.getInt());
                    read_bytes_size += 8;
                }
                break;
            }
            case ValueType::vtFloat32:
                assert_cast<ColumnFloat32 &>(column).insertValue(static_cast<Float32>(value.getDouble()));
                read_bytes_size += 4;
                break;
            case ValueType::vtFloat64:
                assert_cast<ColumnFloat64 &>(column).insertValue(value.getDouble());
                read_bytes_size += 8;
                break;
            case ValueType::vtEnum8:
                assert_cast<ColumnInt8 &>(column).insertValue(assert_cast<const DataTypeEnum<Int8> &>(data_type).castToValue(value.data()).safeGet<Int8>());
                read_bytes_size += assert_cast<ColumnInt8 &>(column).byteSize();
                break;
            case ValueType::vtEnum16:
                assert_cast<ColumnInt16 &>(column).insertValue(assert_cast<const DataTypeEnum<Int16> &>(data_type).castToValue(value.data()).safeGet<Int16>());
                read_bytes_size += assert_cast<ColumnInt16 &>(column).byteSize();
                break;
            case ValueType::vtString:
                assert_cast<ColumnString &>(column).insertData(value.data(), value.size());
                read_bytes_size += assert_cast<ColumnString &>(column).byteSize();
                break;
            case ValueType::vtDate:
                assert_cast<ColumnUInt16 &>(column).insertValue(UInt16(value.getDate().getDayNum()));
                read_bytes_size += 2;
                break;
            case ValueType::vtDate32:
                assert_cast<ColumnInt32 &>(column).insertValue(Int32(value.getDate().getExtenedDayNum()));
                read_bytes_size += 4;
                break;
            case ValueType::vtDateTime:
            {
                ReadBufferFromString in(value);
                time_t time = 0;
                readDateTimeText(time, in, assert_cast<const DataTypeDateTime &>(data_type).getTimeZone());
                time = std::max<time_t>(time, 0);
                assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(time));
                read_bytes_size += 4;
                break;
            }
            case ValueType::vtUUID:
                assert_cast<ColumnUUID &>(column).insert(parse<UUID>(value.data(), value.size()));
                read_bytes_size += assert_cast<ColumnUUID &>(column).byteSize();
                break;
            case ValueType::vtDateTime64:[[fallthrough]];
            case ValueType::vtDecimal32: [[fallthrough]];
            case ValueType::vtDecimal64: [[fallthrough]];
            case ValueType::vtDecimal128:[[fallthrough]];
            case ValueType::vtDecimal256:
            {
                ReadBuffer buffer(const_cast<char *>(value.data()), value.size(), 0);
                data_type.getDefaultSerialization()->deserializeWholeText(column, buffer, FormatSettings{});
                read_bytes_size += column.sizeOfValueIfFixed();
                break;
            }
            case ValueType::vtFixedString:
                assert_cast<ColumnFixedString &>(column).insertData(value.data(), value.size());
                read_bytes_size += column.sizeOfValueIfFixed();
                break;
            case ValueType::vtPoint:
            {
                /// The value is 25 bytes:
                /// 4 bytes for integer SRID (0)
                /// 1 byte for integer byte order (1 = little-endian)
                /// 4 bytes for integer type information (1 = Point)
                /// 8 bytes for double-precision X coordinate
                /// 8 bytes for double-precision Y coordinate
                ReadBufferFromMemory payload(value.data(), value.size());
                payload.ignore(4);

                UInt8 endian;
                readBinary(endian, payload);

                Int32 point_type;
                readBinary(point_type, payload);
                if (point_type != 1)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only Point data type is supported");

                Float64 x, y;
                if (endian == 1)
                {
                    readBinaryLittleEndian(x, payload);
                    readBinaryLittleEndian(y, payload);
                }
                else
                {
                    readBinaryBigEndian(x, payload);
                    readBinaryBigEndian(y, payload);
                }

                assert_cast<ColumnTuple &>(column).insert(Tuple({Field(x), Field(y)}));
                read_bytes_size += value.size();
                break;
            }
            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported value type");
        }
    }

    void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }
}


Chunk MySQLSource::generate()
{
    auto row = connection->result.fetch();
    if (!row)
    {
        if (settings->auto_close)
           connection->entry.disconnect();

        return {};
    }

    MutableColumns columns(description.sample_block.columns());
    for (const auto i : collections::range(0, columns.size()))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    size_t num_rows = 0;
    size_t read_bytes_size = 0;

    while (row)
    {
        for (size_t index = 0; index < position_mapping.size(); ++index)
        {
            const auto value = row[position_mapping[index]];
            const auto & sample = description.sample_block.getByPosition(index);

            bool is_type_nullable = description.types[index].second;

            if (!value.isNull())
            {
                if (is_type_nullable)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[index]);
                    const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);
                    insertValue(*data_type.getNestedType(), column_nullable.getNestedColumn(), description.types[index].first, value, read_bytes_size, row.getFieldType(position_mapping[index]));
                    column_nullable.getNullMapData().emplace_back(false);
                }
                else
                {
                    insertValue(*sample.type, *columns[index], description.types[index].first, value, read_bytes_size, row.getFieldType(position_mapping[index]));
                }
            }
            else
            {
                insertDefaultValue(*columns[index], *sample.column);

                if (is_type_nullable)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[index]);
                    column_nullable.getNullMapData().back() = true;
                }
            }
        }

        ++num_rows;
        if (num_rows == settings->max_read_mysql_row_nums || (settings->max_read_mysql_bytes_size && read_bytes_size >= settings->max_read_mysql_bytes_size))
            break;

        row = connection->result.fetch();
    }
    return Chunk(std::move(columns), num_rows);
}

void MySQLSource::initPositionMappingFromQueryResultStructure()
{
    position_mapping.resize(description.sample_block.columns());

    if (!settings->fetch_by_name)
    {
        if (description.sample_block.columns() != connection->result.getNumFields())
            throw Exception(
                ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
                "mysqlxx::UseQueryResult contains {} columns while {} expected",
                connection->result.getNumFields(),
                description.sample_block.columns());

        for (const auto idx : collections::range(0, connection->result.getNumFields()))
            position_mapping[idx] = idx;
    }
    else
    {
        const auto & sample_names = description.sample_block.getNames();
        std::unordered_set<std::string> missing_names(sample_names.begin(), sample_names.end());

        size_t fields_size = connection->result.getNumFields();

        for (const size_t & idx : collections::range(0, fields_size))
        {
            const auto & field_name = connection->result.getFieldName(idx);
            if (description.sample_block.has(field_name))
            {
                const auto & position = description.sample_block.getPositionByName(field_name);
                position_mapping[position] = idx;
                missing_names.erase(field_name);
            }
        }

        if (!missing_names.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
                "mysqlxx::UseQueryResult must contain columns: {}",
                fmt::join(missing_names, ", "));
    }
}

}

#endif
