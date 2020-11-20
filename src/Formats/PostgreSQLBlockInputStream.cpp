#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <vector>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/assert_cast.h>
#include <ext/range.h>
#include "PostgreSQLBlockInputStream.h"
#include <common/logger_useful.h>
#include <Core/Field.h>

namespace DB
{

PostgreSQLBlockInputStream::PostgreSQLBlockInputStream(
    std::shared_ptr<pqxx::connection> connection_,
    const std::string & query_str_,
    const Block & sample_block,
    const UInt64 max_block_size_)
    : query_str(query_str_)
    , max_block_size(max_block_size_)
    , connection(connection_)
    , work(std::make_unique<pqxx::work>(*connection))
    , stream(std::make_unique<pqxx::stream_from>(*work, pqxx::from_query, std::string_view(query_str)))
{
    description.init(sample_block);
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
            stream->complete();
            break;
        }

        if (row->empty())
            break;

        std::string value;
        for (const auto idx : ext::range(0, row->size()))
        {
            value = std::string((*row)[idx]);
            LOG_DEBUG((&Poco::Logger::get("PostgreSQL")), "GOT {}", value);
            const auto & sample = description.sample_block.getByPosition(idx);

            if (value.data())
            {
                if (description.types[idx].second)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);
                    insertValue(column_nullable.getNestedColumn(), value, description.types[idx].first, data_type.getNestedType());
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                {
                    insertValue(*columns[idx], value, description.types[idx].first, sample.type);
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


void PostgreSQLBlockInputStream::insertValue(IColumn & column, const std::string & value,
        const ExternalResultDescription::ValueType type, const DataTypePtr data_type)
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
        case ValueType::vtDate:
            //assert_cast<ColumnUInt16 &>(column).insertValue(UInt16(value.getDate().getDayNum()));
            break;
        case ValueType::vtDateTime:
            //assert_cast<ColumnUInt32 &>(column).insertValue(UInt32(value.getDateTime()));
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
            data_type->deserializeAsWholeText(column, buffer, FormatSettings{});
            break;
        }
        case ValueType::vtArray:
        {
            const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get());
            auto nested = array_type->getNestedType();

            size_t expected_dimensions = 1;
            while (isArray(nested))
            {
                ++expected_dimensions;
                nested = typeid_cast<const DataTypeArray *>(nested.get())->getNestedType();
            }
            auto which = WhichDataType(nested);

            auto get_array([&]() -> Field
            {
                pqxx::array_parser parser{value};
                std::pair<pqxx::array_parser::juncture, std::string> parsed = parser.get_next();

                std::vector<std::vector<Field>> dimensions(expected_dimensions + 1);
                size_t dimension = 0, max_dimension = 0;
                bool new_row = false, null_value = false;

                while (parsed.first != pqxx::array_parser::juncture::done)
                {
                    while (parsed.first == pqxx::array_parser::juncture::row_start)
                    {
                        ++dimension;
                        if (dimension > expected_dimensions)
                            throw Exception("Got more dimensions than expected", ErrorCodes::BAD_ARGUMENTS);

                        parsed = parser.get_next();
                        new_row = true;
                    }

                    /// TODO: dont forget to add test with null type
                    std::vector<Field> current_dimension_row;
                    while (parsed.first != pqxx::array_parser::juncture::row_end)
                    {
                        if (parsed.first == pqxx::array_parser::juncture::null_value)
                            null_value = true;

                        if (which.isUInt8() || which.isUInt16())
                            current_dimension_row.emplace_back(!null_value ? pqxx::from_string<uint16_t>(parsed.second) : UInt16());
                        else if (which.isUInt32())
                            current_dimension_row.emplace_back(!null_value ? pqxx::from_string<uint32_t>(parsed.second) : UInt32());
                        else if (which.isUInt64())
                            current_dimension_row.emplace_back(!null_value ? pqxx::from_string<uint64_t>(parsed.second) : UInt64());
                        else if (which.isInt8() || which.isInt16())
                            current_dimension_row.emplace_back(!null_value ? pqxx::from_string<int16_t>(parsed.second) : Int16());
                        else if (which.isInt32())
                            current_dimension_row.emplace_back(!null_value ? pqxx::from_string<int32_t>(parsed.second) : Int32());
                        else if (which.isInt64())
                            current_dimension_row.emplace_back(!null_value ? pqxx::from_string<int64_t>(parsed.second) : Int64());
                        //else if (which.isDate())
                        //else if (which.isDateTime())
                        else if (which.isFloat32())
                            current_dimension_row.emplace_back(!null_value ? pqxx::from_string<float>(parsed.second) : Float32());
                        else if (which.isFloat64())
                            current_dimension_row.emplace_back(!null_value ? pqxx::from_string<double>(parsed.second) : Float64());
                        else if (which.isString() || which.isFixedString())
                            current_dimension_row.emplace_back(!null_value ? parsed.second : String());
                        else throw Exception("Unexpected type " + nested->getName(), ErrorCodes::BAD_ARGUMENTS);

                        parsed = parser.get_next();
                        null_value = false;
                    }

                    while (parsed.first == pqxx::array_parser::juncture::row_end)
                    {
                        --dimension;
                        if (std::exchange(new_row, false))
                        {
                            if (dimension + 1 > max_dimension)
                                max_dimension = dimension + 1;
                            if (dimension)
                                dimensions[dimension].emplace_back(Array(current_dimension_row.begin(), current_dimension_row.end()));
                            else
                                return Array(current_dimension_row.begin(), current_dimension_row.end());
                        }
                        else if (dimension)
                        {
                            dimensions[dimension].emplace_back(Array(dimensions[dimension + 1].begin(), dimensions[dimension + 1].end()));
                            dimensions[dimension + 1].clear();
                        }
                        parsed = parser.get_next();
                    }
                }

                if (max_dimension < expected_dimensions)
                    throw Exception("Got less dimensions than expected", ErrorCodes::BAD_ARGUMENTS);

                return Array(dimensions[1].begin(), dimensions[1].end());
            });

            assert_cast<ColumnArray &>(column).insert(get_array());
            break;
        }
    }
}

}

#endif
