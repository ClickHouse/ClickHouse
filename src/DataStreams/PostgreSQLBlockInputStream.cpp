#include "PostgreSQLBlockInputStream.h"

#if USE_LIBPQXX
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Common/assert_cast.h>
#include <ext/range.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TYPE;
}


PostgreSQLBlockInputStream::PostgreSQLBlockInputStream(
    ConnectionPtr connection_,
    const std::string & query_str_,
    const Block & sample_block,
    const UInt64 max_block_size_)
    : query_str(query_str_)
    , max_block_size(max_block_size_)
    , connection(connection_)
{
    description.init(sample_block);
    for (const auto idx : ext::range(0, description.sample_block.columns()))
        if (description.types[idx].first == ValueType::vtArray)
            prepareArrayInfo(idx, description.sample_block.getByPosition(idx).type);
    /// pqxx::stream_from uses COPY command, but when selecting from dictionary will get ';', it is not needed
    if (query_str.ends_with(';'))
        query_str.resize(query_str.size() - 1);
}


void PostgreSQLBlockInputStream::readPrefix()
{
    tx = std::make_unique<pqxx::read_transaction>(*connection);
    stream = std::make_unique<pqxx::stream_from>(*tx, pqxx::from_query, std::string_view(query_str));
}


Block PostgreSQLBlockInputStream::readImpl()
{
    /// Check if pqxx::stream_from is finished
    if (!stream || !(*stream))
        return Block();

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (true)
    {
        const std::vector<pqxx::zview> * row{stream->read_row()};

        if (!row)
        {
            /// row is nullptr if pqxx::stream_from is finished
            stream->complete();
            tx->commit();
            break;
        }

        for (const auto idx : ext::range(0, row->size()))
        {
            const auto & sample = description.sample_block.getByPosition(idx);

            /// if got NULL type, then pqxx::zview will return nullptr in c_str()
            if ((*row)[idx].c_str())
            {
                if (description.types[idx].second)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);
                    insertValue(column_nullable.getNestedColumn(), (*row)[idx], description.types[idx].first, data_type.getNestedType(), idx);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                {
                    insertValue(*columns[idx], (*row)[idx], description.types[idx].first, sample.type, idx);
                }
            }
            else
            {
                insertDefaultValue(*columns[idx], *sample.column);
            }

        }

        if (++num_rows == max_block_size)
            break;
    }

    return description.sample_block.cloneWithColumns(std::move(columns));
}


void PostgreSQLBlockInputStream::insertValue(IColumn & column, std::string_view value,
        const ExternalResultDescription::ValueType type, const DataTypePtr data_type, size_t idx)
{
    switch (type)
    {
        case ValueType::vtUInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(pqxx::from_string<uint16_t>(value));
            break;
        case ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(pqxx::from_string<uint16_t>(value));
            break;
        case ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(pqxx::from_string<uint32_t>(value));
            break;
        case ValueType::vtUInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(pqxx::from_string<uint64_t>(value));
            break;
        case ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(pqxx::from_string<int16_t>(value));
            break;
        case ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(pqxx::from_string<int16_t>(value));
            break;
        case ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(pqxx::from_string<int32_t>(value));
            break;
        case ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(pqxx::from_string<int64_t>(value));
            break;
        case ValueType::vtFloat32:
            assert_cast<ColumnFloat32 &>(column).insertValue(pqxx::from_string<float>(value));
            break;
        case ValueType::vtFloat64:
            assert_cast<ColumnFloat64 &>(column).insertValue(pqxx::from_string<double>(value));
            break;
        case ValueType::vtString:
            assert_cast<ColumnString &>(column).insertData(value.data(), value.size());
            break;
        case ValueType::vtUUID:
            assert_cast<ColumnUInt128 &>(column).insert(parse<UUID>(value.data(), value.size()));
            break;
        case ValueType::vtDate:
            assert_cast<ColumnUInt16 &>(column).insertValue(UInt16{LocalDate{std::string(value)}.getDayNum()});
            break;
        case ValueType::vtDateTime:
            assert_cast<ColumnUInt32 &>(column).insertValue(time_t{LocalDateTime{std::string(value)}});
            break;
        case ValueType::vtDateTime64:[[fallthrough]];
        case ValueType::vtDecimal32: [[fallthrough]];
        case ValueType::vtDecimal64: [[fallthrough]];
        case ValueType::vtDecimal128:
        {
            ReadBufferFromString istr(value);
            data_type->deserializeAsWholeText(column, istr, FormatSettings{});
            break;
        }
        case ValueType::vtArray:
        {
            pqxx::array_parser parser{value};
            std::pair<pqxx::array_parser::juncture, std::string> parsed = parser.get_next();

            size_t dimension = 0, max_dimension = 0, expected_dimensions = array_info[idx].num_dimensions;
            const auto parse_value = array_info[idx].pqxx_parser;
            std::vector<std::vector<Field>> dimensions(expected_dimensions + 1);

            while (parsed.first != pqxx::array_parser::juncture::done)
            {
                if ((parsed.first == pqxx::array_parser::juncture::row_start) && (++dimension > expected_dimensions))
                    throw Exception("Got more dimensions than expected", ErrorCodes::BAD_ARGUMENTS);

                else if (parsed.first == pqxx::array_parser::juncture::string_value)
                    dimensions[dimension].emplace_back(parse_value(parsed.second));

                else if (parsed.first == pqxx::array_parser::juncture::null_value)
                    dimensions[dimension].emplace_back(array_info[idx].default_value);

                else if (parsed.first == pqxx::array_parser::juncture::row_end)
                {
                    max_dimension = std::max(max_dimension, dimension);

                    if (--dimension == 0)
                        break;

                    dimensions[dimension].emplace_back(Array(dimensions[dimension + 1].begin(), dimensions[dimension + 1].end()));
                    dimensions[dimension + 1].clear();
                }

                parsed = parser.get_next();
            }

            if (max_dimension < expected_dimensions)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Got less dimensions than expected. ({} instead of {})", max_dimension, expected_dimensions);

            assert_cast<ColumnArray &>(column).insert(Array(dimensions[1].begin(), dimensions[1].end()));
            break;
        }
        default:
            throw Exception("Value of unsupported type:" + column.getName(), ErrorCodes::UNKNOWN_TYPE);
    }
}


void PostgreSQLBlockInputStream::prepareArrayInfo(size_t column_idx, const DataTypePtr data_type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get());
    auto nested = array_type->getNestedType();

    size_t count_dimensions = 1;
    while (isArray(nested))
    {
        ++count_dimensions;
        nested = typeid_cast<const DataTypeArray *>(nested.get())->getNestedType();
    }

    Field default_value = nested->getDefault();
    if (nested->isNullable())
        nested = typeid_cast<const DataTypeNullable *>(nested.get())->getNestedType();

    WhichDataType which(nested);
    std::function<Field(std::string & fields)> parser;

    if (which.isUInt8() || which.isUInt16())
        parser = [](std::string & field) -> Field { return pqxx::from_string<uint16_t>(field); };
    else if (which.isUInt32())
        parser = [](std::string & field) -> Field { return pqxx::from_string<uint16_t>(field); };
    else if (which.isUInt64())
        parser = [](std::string & field) -> Field { return pqxx::from_string<uint64_t>(field); };
    else if (which.isInt8() || which.isInt16())
        parser = [](std::string & field) -> Field { return pqxx::from_string<int16_t>(field); };
    else if (which.isInt32())
        parser = [](std::string & field) -> Field { return pqxx::from_string<int32_t>(field); };
    else if (which.isInt64())
        parser = [](std::string & field) -> Field { return pqxx::from_string<uint16_t>(field); };
    else if (which.isFloat32())
        parser = [](std::string & field) -> Field { return pqxx::from_string<float>(field); };
    else if (which.isFloat64())
        parser = [](std::string & field) -> Field { return pqxx::from_string<double>(field); };
    else if (which.isString() || which.isFixedString())
        parser = [](std::string & field) -> Field { return field; };
    else if (which.isDate())
        parser = [](std::string & field) -> Field { return UInt16{LocalDate{field}.getDayNum()}; };
    else if (which.isDateTime())
        parser = [](std::string & field) -> Field { return time_t{LocalDateTime{field}}; };
    else if (which.isDecimal32())
        parser = [nested](std::string & field) -> Field
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal32> *>(nested.get());
            DataTypeDecimal<Decimal32> res(getDecimalPrecision(*type), getDecimalScale(*type));
            return convertFieldToType(field, res);
        };
    else if (which.isDecimal64())
        parser = [nested](std::string & field) -> Field
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal64> *>(nested.get());
            DataTypeDecimal<Decimal64> res(getDecimalPrecision(*type), getDecimalScale(*type));
            return convertFieldToType(field, res);
        };
    else if (which.isDecimal128())
        parser = [nested](std::string & field) -> Field
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal128> *>(nested.get());
            DataTypeDecimal<Decimal128> res(getDecimalPrecision(*type), getDecimalScale(*type));
            return convertFieldToType(field, res);
        };
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Type conversion to {} is not supported", nested->getName());

    array_info[column_idx] = {count_dimensions, default_value, parser};
}

}

#endif
