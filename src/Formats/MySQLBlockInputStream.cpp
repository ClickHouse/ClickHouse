#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#    include <vector>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsNumber.h>
#    include <Columns/ColumnDecimal.h>
#    include <DataTypes/IDataType.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <IO/ReadHelpers.h>
#    include <IO/WriteHelpers.h>
#    include <Common/assert_cast.h>
#    include <ext/range.h>
#    include "MySQLBlockInputStream.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

MySQLBlockInputStream::Connection::Connection(
    const mysqlxx::PoolWithFailover::Entry & entry_,
    const std::string & query_str)
    : entry(entry_)
    , query{entry->query(query_str)}
    , result{query.use()}
{
}

MySQLBlockInputStream::MySQLBlockInputStream(
    const mysqlxx::PoolWithFailover::Entry & entry,
    const std::string & query_str,
    const Block & sample_block,
    const UInt64 max_block_size_,
    const bool auto_close_)
    : connection{std::make_unique<Connection>(entry, query_str)}
    , max_block_size{max_block_size_}
    , auto_close{auto_close_}
{
    if (sample_block.columns() != connection->result.getNumFields())
        throw Exception{"mysqlxx::UseQueryResult contains " + toString(connection->result.getNumFields()) + " columns while "
                            + toString(sample_block.columns()) + " expected",
                        ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH};

    description.init(sample_block);
}


namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    void insertValue(const IDataType & data_type, IColumn & column, const ValueType type, const mysqlxx::Value & value)
    {
        switch (type)
        {
            case ValueType::vtUInt8:
                assert_cast<ColumnUInt8 &>(column).insertValue(value.getUInt());
                break;
            case ValueType::vtUInt16:
                assert_cast<ColumnUInt16 &>(column).insertValue(value.getUInt());
                break;
            case ValueType::vtUInt32:
                assert_cast<ColumnUInt32 &>(column).insertValue(value.getUInt());
                break;
            case ValueType::vtUInt64:
                assert_cast<ColumnUInt64 &>(column).insertValue(value.getUInt());
                break;
            case ValueType::vtInt8:
                assert_cast<ColumnInt8 &>(column).insertValue(value.getInt());
                break;
            case ValueType::vtInt16:
                assert_cast<ColumnInt16 &>(column).insertValue(value.getInt());
                break;
            case ValueType::vtInt32:
                assert_cast<ColumnInt32 &>(column).insertValue(value.getInt());
                break;
            case ValueType::vtInt64:
                assert_cast<ColumnInt64 &>(column).insertValue(value.getInt());
                break;
            case ValueType::vtFloat32:
                assert_cast<ColumnFloat32 &>(column).insertValue(value.getDouble());
                break;
            case ValueType::vtFloat64:
                assert_cast<ColumnFloat64 &>(column).insertValue(value.getDouble());
                break;
            case ValueType::vtString:
                assert_cast<ColumnString &>(column).insertData(value.data(), value.size());
                break;
            case ValueType::vtDate:
                assert_cast<ColumnUInt16 &>(column).insertValue(UInt16(value.getDate().getDayNum()));
                break;
            case ValueType::vtDateTime:
                assert_cast<ColumnUInt32 &>(column).insertValue(UInt32(value.getDateTime()));
                break;
            case ValueType::vtUUID:
                assert_cast<ColumnUInt128 &>(column).insert(parse<UUID>(value.data(), value.size()));
                break;
            case ValueType::vtDateTime64:[[fallthrough]];
            case ValueType::vtDecimal32: [[fallthrough]];
            case ValueType::vtDecimal64: [[fallthrough]];
            case ValueType::vtDecimal128:[[fallthrough]];
            case ValueType::vtDecimal256:
            {
                ReadBuffer buffer(const_cast<char *>(value.data()), value.size(), 0);
                data_type.deserializeAsWholeText(column, buffer, FormatSettings{});
                break;
            }
        }
    }

    void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }
}


Block MySQLBlockInputStream::readImpl()
{
    auto row = connection->result.fetch();
    if (!row)
    {
        if (auto_close)
           connection->entry.disconnect();
        return {};
    }

    MutableColumns columns(description.sample_block.columns());
    for (const auto i : ext::range(0, columns.size()))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    size_t num_rows = 0;
    while (row)
    {
        for (const auto idx : ext::range(0, row.size()))
        {
            const auto value = row[idx];
            const auto & sample = description.sample_block.getByPosition(idx);
            if (!value.isNull())
            {
                if (description.types[idx].second)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);
                    insertValue(*data_type.getNestedType(), column_nullable.getNestedColumn(), description.types[idx].first, value);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                    insertValue(*sample.type, *columns[idx], description.types[idx].first, value);
            }
            else
                insertDefaultValue(*columns[idx], *sample.column);
        }

        ++num_rows;
        if (num_rows == max_block_size)
            break;

        row = connection->result.fetch();
    }
    return description.sample_block.cloneWithColumns(std::move(columns));
}

MySQLBlockInputStream::MySQLBlockInputStream(
    const Block & sample_block_,
    UInt64 max_block_size_,
    bool auto_close_)
    : max_block_size(max_block_size_)
    , auto_close(auto_close_)
{
    description.init(sample_block_);
}

MySQLLazyBlockInputStream::MySQLLazyBlockInputStream(
    mysqlxx::Pool & pool_,
    const std::string & query_str_,
    const Block & sample_block_,
    const UInt64 max_block_size_,
    const bool auto_close_)
    : MySQLBlockInputStream(sample_block_, max_block_size_, auto_close_)
    , pool(pool_)
    , query_str(query_str_)
{
}

void MySQLLazyBlockInputStream::readPrefix()
{
    connection = std::make_unique<Connection>(pool.get(), query_str);
    if (description.sample_block.columns() != connection->result.getNumFields())
        throw Exception{"mysqlxx::UseQueryResult contains " + toString(connection->result.getNumFields()) + " columns while "
                            + toString(description.sample_block.columns()) + " expected",
                        ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH};
}

}

#endif
