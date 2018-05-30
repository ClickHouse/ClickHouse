#include <Dictionaries/ODBCBlockInputStream.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>

#include <common/logger_useful.h>
#include <ext/range.h>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


ODBCBlockInputStream::ODBCBlockInputStream(
    Poco::Data::Session && session, const std::string & query_str, const Block & sample_block,
    const size_t max_block_size)
    :
    session{session},
    statement{(this->session << query_str, Poco::Data::Keywords::now)},
    result{statement},
    iterator{result.begin()},
    max_block_size{max_block_size},
    log(&Logger::get("ODBCBlockInputStream"))
{
    if (sample_block.columns() != result.columnCount())
        throw Exception{"RecordSet contains " + toString(result.columnCount()) + " columns while " +
            toString(sample_block.columns()) + " expected", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH};

    description.init(sample_block);
}


namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    static void insertValue(IColumn & column, const ValueType type, const Poco::Dynamic::Var & value)
    {
        switch (type)
        {
            case ValueType::UInt8: static_cast<ColumnUInt8 &>(column).insert(value.convert<UInt64>()); break;
            case ValueType::UInt16: static_cast<ColumnUInt16 &>(column).insert(value.convert<UInt64>()); break;
            case ValueType::UInt32: static_cast<ColumnUInt32 &>(column).insert(value.convert<UInt64>()); break;
            case ValueType::UInt64: static_cast<ColumnUInt64 &>(column).insert(value.convert<UInt64>()); break;
            case ValueType::Int8: static_cast<ColumnInt8 &>(column).insert(value.convert<Int64>()); break;
            case ValueType::Int16: static_cast<ColumnInt16 &>(column).insert(value.convert<Int64>()); break;
            case ValueType::Int32: static_cast<ColumnInt32 &>(column).insert(value.convert<Int64>()); break;
            case ValueType::Int64: static_cast<ColumnInt64 &>(column).insert(value.convert<Int64>()); break;
            case ValueType::Float32: static_cast<ColumnFloat32 &>(column).insert(value.convert<Float64>()); break;
            case ValueType::Float64: static_cast<ColumnFloat64 &>(column).insert(value.convert<Float64>()); break;
            case ValueType::String: static_cast<ColumnString &>(column).insert(value.convert<String>()); break;
            case ValueType::Date: static_cast<ColumnUInt16 &>(column).insert(UInt16{LocalDate{value.convert<String>()}.getDayNum()}); break;
            case ValueType::DateTime: static_cast<ColumnUInt32 &>(column).insert(time_t{LocalDateTime{value.convert<String>()}}); break;
        }
    }

    void insertDefaultValue(IColumn & column, const IColumn & sample_column)
    {
        column.insertFrom(sample_column, 0);
    }
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
                insertValue(*columns[idx], description.types[idx], value);
            else
                insertDefaultValue(*columns[idx], *description.sample_columns[idx]);
        }

        ++iterator;

        ++num_rows;
        if (num_rows == max_block_size)
            break;

    }

    return description.sample_block.cloneWithColumns(std::move(columns));
}

}
