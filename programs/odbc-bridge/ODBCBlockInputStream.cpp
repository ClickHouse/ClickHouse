#include "ODBCBlockInputStream.h"
#include <vector>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>
#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


ODBCBlockInputStream::ODBCBlockInputStream(
    Poco::Data::Session && session_, const std::string & query_str, const Block & sample_block, const UInt64 max_block_size_)
    : session{session_}
    , statement{(this->session << query_str, Poco::Data::Keywords::now)}
    , result{statement}
    , iterator{result.begin()}
    , max_block_size{max_block_size_}
    , log(&Poco::Logger::get("ODBCBlockInputStream"))
{
    if (sample_block.columns() != result.columnCount())
        throw Exception{"RecordSet contains " + toString(result.columnCount()) + " columns while " + toString(sample_block.columns())
                            + " expected",
                        ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH};

    description.init(sample_block);
}


namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    void insertValue(IColumn & column, const ValueType type, const Poco::Dynamic::Var & value)
    {
        switch (type)
        {
            case ValueType::vtUInt8:
                assert_cast<ColumnUInt8 &>(column).insertValue(value.convert<UInt64>());
                break;
            case ValueType::vtUInt16:
                assert_cast<ColumnUInt16 &>(column).insertValue(value.convert<UInt64>());
                break;
            case ValueType::vtUInt32:
                assert_cast<ColumnUInt32 &>(column).insertValue(value.convert<UInt64>());
                break;
            case ValueType::vtUInt64:
                assert_cast<ColumnUInt64 &>(column).insertValue(value.convert<UInt64>());
                break;
            case ValueType::vtInt8:
                assert_cast<ColumnInt8 &>(column).insertValue(value.convert<Int64>());
                break;
            case ValueType::vtInt16:
                assert_cast<ColumnInt16 &>(column).insertValue(value.convert<Int64>());
                break;
            case ValueType::vtInt32:
                assert_cast<ColumnInt32 &>(column).insertValue(value.convert<Int64>());
                break;
            case ValueType::vtInt64:
                assert_cast<ColumnInt64 &>(column).insertValue(value.convert<Int64>());
                break;
            case ValueType::vtFloat32:
                assert_cast<ColumnFloat32 &>(column).insertValue(value.convert<Float64>());
                break;
            case ValueType::vtFloat64:
                assert_cast<ColumnFloat64 &>(column).insertValue(value.convert<Float64>());
                break;
            case ValueType::vtString:
                assert_cast<ColumnString &>(column).insert(value.convert<String>());
                break;
            case ValueType::vtDate:
                assert_cast<ColumnUInt16 &>(column).insertValue(UInt16{LocalDate{value.convert<String>()}.getDayNum()});
                break;
            case ValueType::vtDateTime:
                assert_cast<ColumnUInt32 &>(column).insertValue(time_t{LocalDateTime{value.convert<String>()}});
                break;
            case ValueType::vtUUID:
                assert_cast<ColumnUInt128 &>(column).insert(parse<UUID>(value.convert<std::string>()));
                break;
        }
    }

    void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }
}


Block ODBCBlockInputStream::readImpl()
{
    if (iterator == result.end())
        return {};

    MutableColumns columns(description.sample_block.columns());
    for (const auto i : ext::range(0, columns.size()))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    size_t num_rows = 0;
    while (iterator != result.end())
    {
        Poco::Data::Row & row = *iterator;

        for (const auto idx : ext::range(0, row.fieldCount()))
        {
            const Poco::Dynamic::Var & value = row[idx];

            if (!value.isEmpty())
            {
                if (description.types[idx].second)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    insertValue(column_nullable.getNestedColumn(), description.types[idx].first, value);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                    insertValue(*columns[idx], description.types[idx].first, value);
            }
            else
                insertDefaultValue(*columns[idx], *description.sample_block.getByPosition(idx).column);
        }

        ++iterator;

        ++num_rows;
        if (num_rows == max_block_size)
            break;
    }

    return description.sample_block.cloneWithColumns(std::move(columns));
}

}
