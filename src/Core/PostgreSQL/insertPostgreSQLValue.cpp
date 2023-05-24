#include "insertPostgreSQLValue.h"

#if USE_LIBPQXX
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Common/assert_cast.h>
#include <pqxx/pqxx>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


void insertDefaultPostgreSQLValue(IColumn & column, const IColumn & sample_column)
{
    column.insertFrom(sample_column, 0);
}


void insertPostgreSQLValue(
        IColumn & column, std::string_view value,
        const ExternalResultDescription::ValueType type, const DataTypePtr data_type,
        std::unordered_map<size_t, PostgreSQLArrayInfo> & array_info, size_t idx)
{
    switch (type)
    {

        case ExternalResultDescription::ValueType::vtUInt8:
        {
            if (value == "t")
                assert_cast<ColumnUInt8 &>(column).insertValue(1);
            else if (value == "f")
                assert_cast<ColumnUInt8 &>(column).insertValue(0);
            else
                assert_cast<ColumnUInt8 &>(column).insertValue(pqxx::from_string<uint16_t>(value));
            break;
        }
        case ExternalResultDescription::ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(pqxx::from_string<uint16_t>(value));
            break;
        case ExternalResultDescription::ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(pqxx::from_string<uint32_t>(value));
            break;
        case ExternalResultDescription::ValueType::vtUInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(pqxx::from_string<uint64_t>(value));
            break;
        case ExternalResultDescription::ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(pqxx::from_string<int16_t>(value));
            break;
        case ExternalResultDescription::ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(pqxx::from_string<int16_t>(value));
            break;
        case ExternalResultDescription::ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(pqxx::from_string<int32_t>(value));
            break;
        case ExternalResultDescription::ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(pqxx::from_string<int64_t>(value));
            break;
        case ExternalResultDescription::ValueType::vtFloat32:
            assert_cast<ColumnFloat32 &>(column).insertValue(pqxx::from_string<float>(value));
            break;
        case ExternalResultDescription::ValueType::vtFloat64:
            assert_cast<ColumnFloat64 &>(column).insertValue(pqxx::from_string<double>(value));
            break;
        case ExternalResultDescription::ValueType::vtEnum8:
        case ExternalResultDescription::ValueType::vtEnum16:
        case ExternalResultDescription::ValueType::vtFixedString:
        case ExternalResultDescription::ValueType::vtString:
            assert_cast<ColumnString &>(column).insertData(value.data(), value.size());
            break;
        case ExternalResultDescription::ValueType::vtUUID:
            assert_cast<ColumnUUID &>(column).insertValue(parse<UUID>(value.data(), value.size()));
            break;
        case ExternalResultDescription::ValueType::vtDate:
            assert_cast<ColumnUInt16 &>(column).insertValue(UInt16{LocalDate{std::string(value)}.getDayNum()});
            break;
        case ExternalResultDescription::ValueType::vtDate32:
            assert_cast<ColumnInt32 &>(column).insertValue(Int32{LocalDate{std::string(value)}.getExtenedDayNum()});
            break;
        case ExternalResultDescription::ValueType::vtDateTime:
        {
            ReadBufferFromString in(value);
            time_t time = 0;
            readDateTimeText(time, in, assert_cast<const DataTypeDateTime *>(data_type.get())->getTimeZone());
            if (time < 0)
                time = 0;
            assert_cast<ColumnUInt32 &>(column).insertValue(time);
            break;
        }
        case ExternalResultDescription::ValueType::vtDateTime64:
        {
            ReadBufferFromString in(value);
            DateTime64 time = 0;
            readDateTime64Text(time, 6, in, assert_cast<const DataTypeDateTime64 *>(data_type.get())->getTimeZone());
            assert_cast<DataTypeDateTime64::ColumnType &>(column).insertValue(time);
            break;
        }
        case ExternalResultDescription::ValueType::vtDecimal32: [[fallthrough]];
        case ExternalResultDescription::ValueType::vtDecimal64: [[fallthrough]];
        case ExternalResultDescription::ValueType::vtDecimal128: [[fallthrough]];
        case ExternalResultDescription::ValueType::vtDecimal256:
        {
            ReadBufferFromString istr(value);
            data_type->getDefaultSerialization()->deserializeWholeText(column, istr, FormatSettings{});
            break;
        }
        case ExternalResultDescription::ValueType::vtArray:
        {
            pqxx::array_parser parser{value};
            std::pair<pqxx::array_parser::juncture, std::string> parsed = parser.get_next();

            size_t dimension = 0, max_dimension = 0, expected_dimensions = array_info[idx].num_dimensions;
            const auto parse_value = array_info[idx].pqxx_parser;
            std::vector<Row> dimensions(expected_dimensions + 1);

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

                    --dimension;
                    if (dimension == 0)
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
    }
}


void preparePostgreSQLArrayInfo(
        std::unordered_map<size_t, PostgreSQLArrayInfo> & array_info, size_t column_idx, const DataTypePtr data_type)
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
        nested = static_cast<const DataTypeNullable *>(nested.get())->getNestedType();

    WhichDataType which(nested);
    std::function<Field(std::string & fields)> parser;

    if (which.isUInt8() || which.isUInt16())
        parser = [](std::string & field) -> Field { return pqxx::from_string<uint16_t>(field); };
    else if (which.isInt8() || which.isInt16())
        parser = [](std::string & field) -> Field { return pqxx::from_string<int16_t>(field); };
    else if (which.isUInt32())
        parser = [](std::string & field) -> Field { return pqxx::from_string<uint32_t>(field); };
    else if (which.isInt32())
        parser = [](std::string & field) -> Field { return pqxx::from_string<int32_t>(field); };
    else if (which.isUInt64())
        parser = [](std::string & field) -> Field { return pqxx::from_string<uint64_t>(field); };
    else if (which.isInt64())
        parser = [](std::string & field) -> Field { return pqxx::from_string<int64_t>(field); };
    else if (which.isFloat32())
        parser = [](std::string & field) -> Field { return pqxx::from_string<float>(field); };
    else if (which.isFloat64())
        parser = [](std::string & field) -> Field { return pqxx::from_string<double>(field); };
    else if (which.isString() || which.isFixedString())
        parser = [](std::string & field) -> Field { return field; };
    else if (which.isDate())
        parser = [](std::string & field) -> Field { return UInt16{LocalDate{field}.getDayNum()}; };
    else if (which.isDateTime())
        parser = [nested](std::string & field) -> Field
        {
            ReadBufferFromString in(field);
            time_t time = 0;
            readDateTimeText(time, in, assert_cast<const DataTypeDateTime *>(nested.get())->getTimeZone());
            if (time < 0)
                time = 0;
            return time;
        };
    else if (which.isDateTime64())
        parser = [nested](std::string & field) -> Field
        {
            ReadBufferFromString in(field);
            DateTime64 time = 0;
            readDateTime64Text(time, 6, in, assert_cast<const DataTypeDateTime64 *>(nested.get())->getTimeZone());
            if (time < 0)
                time = 0;
            return time;
        };
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
    else if (which.isDecimal256())
        parser = [nested](std::string & field) -> Field
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal256> *>(nested.get());
            DataTypeDecimal<Decimal256> res(getDecimalPrecision(*type), getDecimalScale(*type));
            return convertFieldToType(field, res);
        };
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Type conversion to {} is not supported", nested->getName());

    array_info[column_idx] = {count_dimensions, default_value, parser};
}
}

#endif
