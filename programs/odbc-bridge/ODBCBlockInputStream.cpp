#include "ODBCBlockInputStream.h"
#include <vector>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}


ODBCBlockInputStream::ODBCBlockInputStream(
    nanodbc::ConnectionHolderPtr connection_holder, const std::string & query_str, const Block & sample_block, const UInt64 max_block_size_)
    : log(&Poco::Logger::get("ODBCBlockInputStream"))
    , max_block_size{max_block_size_}
    , query(query_str)
{
    description.init(sample_block);
    result = execute<nanodbc::result>(connection_holder,
                     [&](nanodbc::connection & connection) { return execute(connection, query); });
}


Block ODBCBlockInputStream::readImpl()
{
    if (finished)
        return Block();

    MutableColumns columns(description.sample_block.cloneEmptyColumns());
    size_t num_rows = 0;

    while (true)
    {
        if (!result.next())
        {
            finished = true;
            break;
        }

        for (int idx = 0; idx < result.columns(); ++idx)
        {
            const auto & sample = description.sample_block.getByPosition(idx);

            if (!result.is_null(idx))
            {
                bool is_nullable = description.types[idx].second;

                if (is_nullable)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);
                    insertValue(column_nullable.getNestedColumn(), data_type.getNestedType(), description.types[idx].first, result, idx);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                {
                    insertValue(*columns[idx], sample.type, description.types[idx].first, result, idx);
                }
            }
            else
                insertDefaultValue(*columns[idx], *sample.column);
        }

        if (++num_rows == max_block_size)
            break;
    }

    return description.sample_block.cloneWithColumns(std::move(columns));
}


void ODBCBlockInputStream::insertValue(
        IColumn & column, const DataTypePtr data_type, const ValueType type, nanodbc::result & row, size_t idx)
{
    switch (type)
    {
        case ValueType::vtUInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(row.get<uint16_t>(idx));
            break;
        case ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(row.get<uint16_t>(idx));
            break;
        case ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(row.get<uint32_t>(idx));
            break;
        case ValueType::vtUInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(row.get<uint64_t>(idx));
            break;
        case ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(row.get<int16_t>(idx));
            break;
        case ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(row.get<int16_t>(idx));
            break;
        case ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(row.get<int32_t>(idx));
            break;
        case ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(row.get<int64_t>(idx));
            break;
        case ValueType::vtFloat32:
            assert_cast<ColumnFloat32 &>(column).insertValue(row.get<float>(idx));
            break;
        case ValueType::vtFloat64:
            assert_cast<ColumnFloat64 &>(column).insertValue(row.get<double>(idx));
            break;
        case ValueType::vtFixedString:[[fallthrough]];
        case ValueType::vtEnum8:
        case ValueType::vtEnum16:
        case ValueType::vtString:
            assert_cast<ColumnString &>(column).insert(row.get<std::string>(idx));
            break;
        case ValueType::vtUUID:
        {
            auto value = row.get<std::string>(idx);
            assert_cast<ColumnUInt128 &>(column).insert(parse<UUID>(value.data(), value.size()));
            break;
        }
        case ValueType::vtDate:
            assert_cast<ColumnUInt16 &>(column).insertValue(UInt16{LocalDate{row.get<std::string>(idx)}.getDayNum()});
            break;
        case ValueType::vtDateTime:
        {
            auto value = row.get<std::string>(idx);
            ReadBufferFromString in(value);
            time_t time = 0;
            readDateTimeText(time, in, assert_cast<const DataTypeDateTime *>(data_type.get())->getTimeZone());
            if (time < 0)
                time = 0;
            assert_cast<ColumnUInt32 &>(column).insertValue(time);
            break;
        }
        case ValueType::vtDateTime64:[[fallthrough]];
        case ValueType::vtDecimal32: [[fallthrough]];
        case ValueType::vtDecimal64: [[fallthrough]];
        case ValueType::vtDecimal128: [[fallthrough]];
        case ValueType::vtDecimal256:
        {
            auto value = row.get<std::string>(idx);
            ReadBufferFromString istr(value);
            data_type->getDefaultSerialization()->deserializeWholeText(column, istr, FormatSettings{});
            break;
        }
        default:
            throw Exception("Unsupported value type", ErrorCodes::UNKNOWN_TYPE);
    }
}

}
