#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#    include <vector>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsNumber.h>
#    include <Columns/ColumnDecimal.h>
#    include <Columns/ColumnFixedString.h>
#    include <DataTypes/IDataType.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <IO/ReadHelpers.h>
#    include <IO/WriteHelpers.h>
#    include <IO/Operators.h>
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
    const bool auto_close_,
    const bool fetch_by_name_)
    : connection{std::make_unique<Connection>(entry, query_str)}
    , max_block_size{max_block_size_}
    , auto_close{auto_close_}
    , fetch_by_name(fetch_by_name_)
{
    description.init(sample_block);
    initPositionMappingFromQueryResultStructure();
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
            case ValueType::vtFixedString:
                assert_cast<ColumnFixedString &>(column).insertData(value.data(), value.size());
                break;
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
        for (size_t index = 0; index < position_mapping.size(); ++index)
        {
            const auto value = row[position_mapping[index]];
            const auto & sample = description.sample_block.getByPosition(index);

            if (!value.isNull())
            {
                if (description.types[index].second)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[index]);
                    const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);
                    insertValue(*data_type.getNestedType(), column_nullable.getNestedColumn(), description.types[index].first, value);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                    insertValue(*sample.type, *columns[index], description.types[index].first, value);
            }
            else
                insertDefaultValue(*columns[index], *sample.column);
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
    bool auto_close_,
    bool fetch_by_name_)
    : max_block_size(max_block_size_)
    , auto_close(auto_close_)
    , fetch_by_name(fetch_by_name_)
{
    description.init(sample_block_);
}

void MySQLBlockInputStream::initPositionMappingFromQueryResultStructure()
{
    position_mapping.resize(description.sample_block.columns());

    if (!fetch_by_name)
    {
        if (description.sample_block.columns() != connection->result.getNumFields())
            throw Exception{"mysqlxx::UseQueryResult contains " + toString(connection->result.getNumFields()) + " columns while "
                + toString(description.sample_block.columns()) + " expected", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH};

        for (const auto idx : ext::range(0, connection->result.getNumFields()))
            position_mapping[idx] = idx;
    }
    else
    {
        const auto & sample_names = description.sample_block.getNames();
        std::unordered_set<std::string> missing_names(sample_names.begin(), sample_names.end());

        size_t fields_size = connection->result.getNumFields();

        for (const size_t & idx : ext::range(0, fields_size))
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
        {
            WriteBufferFromOwnString exception_message;
            for (auto iter = missing_names.begin(); iter != missing_names.end(); ++iter)
            {
                if (iter != missing_names.begin())
                    exception_message << ", ";
                exception_message << *iter;
            }

            throw Exception("mysqlxx::UseQueryResult must be contain the" + exception_message.str() + " columns.",
                ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
        }
    }
}

MySQLLazyBlockInputStream::MySQLLazyBlockInputStream(
    mysqlxx::Pool & pool_,
    const std::string & query_str_,
    const Block & sample_block_,
    const UInt64 max_block_size_,
    const bool auto_close_,
    const bool fetch_by_name_)
    : MySQLBlockInputStream(sample_block_, max_block_size_, auto_close_, fetch_by_name_)
    , pool(pool_)
    , query_str(query_str_)
{
}

void MySQLLazyBlockInputStream::readPrefix()
{
    connection = std::make_unique<Connection>(pool.get(), query_str);
    initPositionMappingFromQueryResultStructure();
}

}

#endif
