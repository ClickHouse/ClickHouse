#include "ODBCSource.h"
#include <vector>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnNullable.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}


ODBCSource::ODBCSource(
    nanodbc::ConnectionHolderPtr connection_holder, const std::string & query_str, const Block & sample_block, const UInt64 max_block_size_)
    : ISource(sample_block)
    , log(getLogger("ODBCSource"))
    , max_block_size{max_block_size_}
    , query(query_str)
{
    description.init(sample_block);
    result = execute<nanodbc::result>(connection_holder,
                     [&](nanodbc::connection & connection) { return execute(connection, query); });
}


Chunk ODBCSource::generate()
{
    if (is_finished)
        return {};

    MutableColumns columns(description.sample_block.cloneEmptyColumns());
    size_t num_rows = 0;

    while (true)
    {
        if (!result.next())
        {
            is_finished = true;
            break;
        }

        for (int idx = 0; idx < result.columns(); ++idx)
        {
            const auto & sample = description.sample_block.getByPosition(idx);
            if (!result.is_null(idx))
            {
                if (columns[idx]->isNullable())
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    insertValue(column_nullable.getNestedColumn(), removeNullable(sample.type), description.types[idx].first, result, idx);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                     insertValue(*columns[idx], removeNullable(sample.type), description.types[idx].first, result, idx);
            }
            else
                insertDefaultValue(*columns[idx], *sample.column);
        }

        if (++num_rows == max_block_size)
            break;
    }

    return Chunk(std::move(columns), num_rows);
}


void ODBCSource::insertValue(
        IColumn & column, const DataTypePtr data_type, const ValueType type, nanodbc::result & row, size_t idx)
{
    switch (type)
    {
        case ValueType::vtUInt8:
            column.insert(row.get<uint16_t>(idx));
            break;
        case ValueType::vtUInt16:
            column.insert(row.get<uint16_t>(idx));
            break;
        case ValueType::vtUInt32:
            column.insert(row.get<uint32_t>(idx));
            break;
        case ValueType::vtUInt64:
            column.insert(row.get<uint64_t>(idx));
            break;
        case ValueType::vtInt8:
            column.insert(row.get<int16_t>(idx));
            break;
        case ValueType::vtInt16:
            column.insert(row.get<int16_t>(idx));
            break;
        case ValueType::vtInt32:
            column.insert(row.get<int32_t>(idx));
            break;
        case ValueType::vtInt64:
            column.insert(row.get<int64_t>(idx));
            break;
        case ValueType::vtFloat32:
            column.insert(row.get<float>(idx));
            break;
        case ValueType::vtFloat64:
            column.insert(row.get<double>(idx));
            break;
        case ValueType::vtFixedString:
        case ValueType::vtEnum8:
        case ValueType::vtEnum16:
        case ValueType::vtString:
            column.insert(row.get<std::string>(idx));
            break;
        case ValueType::vtUUID:
        {
            auto value = row.get<std::string>(idx);
            column.insert(parse<UUID>(value.data(), value.size()));
            break;
        }
        case ValueType::vtDate:
            column.insert(UInt16{LocalDate{row.get<std::string>(idx)}.getDayNum()});
            break;
        case ValueType::vtDateTime:
        {
            auto value = row.get<std::string>(idx);
            ReadBufferFromString in(value);
            time_t time = 0;
            const DataTypeDateTime & datetime_type = assert_cast<const DataTypeDateTime &>(*data_type);
            readDateTimeText(time, in, datetime_type.getTimeZone());
            time = std::max<time_t>(time, 0);
            column.insert(static_cast<UInt32>(time));
            break;
        }
        case ValueType::vtDateTime64:
        {
            auto value = row.get<std::string>(idx);
            ReadBufferFromString in(value);
            DateTime64 time = 0;
            const DataTypeDateTime64 & datetime_type = assert_cast<const DataTypeDateTime64 &>(*data_type);
            readDateTime64Text(time, datetime_type.getScale(), in, datetime_type.getTimeZone());
            column.insert(time);
            break;
        }
        case ValueType::vtDecimal32:
        case ValueType::vtDecimal64:
        case ValueType::vtDecimal128:
        case ValueType::vtDecimal256:
        {
            auto value = row.get<std::string>(idx);
            ReadBufferFromString istr(value);
            data_type->getDefaultSerialization()->deserializeWholeText(column, istr, FormatSettings{});
            break;
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unsupported value type");
    }
}

}
