#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_CASSANDRA

#include <utility>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ExternalResultDescription.h>
#include <IO/ReadHelpers.h>
#include "CassandraBlockInputStream.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

CassandraBlockInputStream::CassandraBlockInputStream(
    const CassSessionShared & session_,
    const String & query_str,
    const Block & sample_block,
    size_t max_block_size_)
    : session(session_)
    , statement(query_str.c_str(), /*parameters count*/ 0)
    , max_block_size(max_block_size_)
    , has_more_pages(cass_true)
{
    description.init(sample_block);
    cassandraCheck(cass_statement_set_paging_size(statement, max_block_size));
}

void CassandraBlockInputStream::insertValue(IColumn & column, ValueType type, const CassValue * cass_value)
{
    switch (type)
    {
        case ValueType::vtUInt8:
        {
            cass_int8_t value;
            cass_value_get_int8(cass_value, &value);
            assert_cast<ColumnUInt8 &>(column).insertValue(static_cast<UInt8>(value));
            break;
        }
        case ValueType::vtUInt16:
        {
            cass_int16_t value;
            cass_value_get_int16(cass_value, &value);
            assert_cast<ColumnUInt16 &>(column).insertValue(static_cast<UInt16>(value));
            break;
        }
        case ValueType::vtUInt32:
        {
            cass_int32_t value;
            cass_value_get_int32(cass_value, &value);
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(value));
            break;
        }
        case ValueType::vtUInt64:
        {
            cass_int64_t value;
            cass_value_get_int64(cass_value, &value);
            assert_cast<ColumnUInt64 &>(column).insertValue(static_cast<UInt64>(value));
            break;
        }
        case ValueType::vtInt8:
        {
            cass_int8_t value;
            cass_value_get_int8(cass_value, &value);
            assert_cast<ColumnInt8 &>(column).insertValue(value);
            break;
        }
        case ValueType::vtInt16:
        {
            cass_int16_t value;
            cass_value_get_int16(cass_value, &value);
            assert_cast<ColumnInt16 &>(column).insertValue(value);
            break;
        }
        case ValueType::vtInt32:
        {
            cass_int32_t value;
            cass_value_get_int32(cass_value, &value);
            assert_cast<ColumnInt32 &>(column).insertValue(value);
            break;
        }
        case ValueType::vtInt64:
        {
            cass_int64_t value;
            cass_value_get_int64(cass_value, &value);
            assert_cast<ColumnInt64 &>(column).insertValue(value);
            break;
        }
        case ValueType::vtFloat32:
        {
            cass_float_t value;
            cass_value_get_float(cass_value, &value);
            assert_cast<ColumnFloat32 &>(column).insertValue(value);
            break;
        }
        case ValueType::vtFloat64:
        {
            cass_double_t value;
            cass_value_get_double(cass_value, &value);
            assert_cast<ColumnFloat64 &>(column).insertValue(value);
            break;
        }
        case ValueType::vtString:
        {
            const char * value = nullptr;
            size_t value_length;
            cass_value_get_string(cass_value, &value, &value_length);
            assert_cast<ColumnString &>(column).insertData(value, value_length);
            break;
        }
        case ValueType::vtDate:
        {
            cass_uint32_t value;
            cass_value_get_uint32(cass_value, &value);
            assert_cast<ColumnUInt16 &>(column).insertValue(static_cast<UInt16>(value));
            break;
        }
        case ValueType::vtDateTime:
        {
            cass_int64_t value;
            cass_value_get_int64(cass_value, &value);
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(value / 1000));
            break;
        }
        case ValueType::vtUUID:
        {
            CassUuid value;
            cass_value_get_uuid(cass_value, &value);
            std::array<char, CASS_UUID_STRING_LENGTH> uuid_str;
            cass_uuid_string(value, uuid_str.data());
            assert_cast<ColumnUInt128 &>(column).insert(parse<UUID>(uuid_str.data(), uuid_str.size()));
            break;
        }
    }
}

void CassandraBlockInputStream::readPrefix()
{
    result_future = cass_session_execute(*session, statement);
}

Block CassandraBlockInputStream::readImpl()
{
    if (!has_more_pages)
        return {};

    MutableColumns columns = description.sample_block.cloneEmptyColumns();

    cassandraWaitAndCheck(result_future);
    CassResultPtr result = cass_future_get_result(result_future);

    assert(cass_result_column_count(result) == columns.size());

    assertTypes(result);

    has_more_pages = cass_result_has_more_pages(result);
    if (has_more_pages)
    {
        cassandraCheck(cass_statement_set_paging_state(statement, result));
        result_future = cass_session_execute(*session, statement);
    }

    CassIteratorPtr rows_iter = cass_iterator_from_result(result);     /// Points to rows[-1]
    while (cass_iterator_next(rows_iter))
    {
        const CassRow * row = cass_iterator_get_row(rows_iter);
        for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx)
        {
            const CassValue * val = cass_row_get_column(row, col_idx);
            if (cass_value_is_null(val))
                columns[col_idx]->insertDefault();
            else if (description.types[col_idx].second)
            {
                ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[col_idx]);
                insertValue(column_nullable.getNestedColumn(), description.types[col_idx].first, val);
                column_nullable.getNullMapData().emplace_back(0);
            }
            else
                insertValue(*columns[col_idx], description.types[col_idx].first, val);
        }
    }

    assert(cass_result_row_count(result) == columns.front()->size());

    return description.sample_block.cloneWithColumns(std::move(columns));
}

void CassandraBlockInputStream::assertTypes(const CassResultPtr & result)
{
    if (!assert_types)
        return;

    size_t column_count = cass_result_column_count(result);
    for (size_t i = 0; i < column_count; ++i)
    {
        CassValueType expected = CASS_VALUE_TYPE_UNKNOWN;
        String expected_text;

        /// Cassandra does not support unsigned integers (cass_uint32_t is for Date)
        switch (description.types[i].first)
        {
            case ExternalResultDescription::ValueType::vtInt8:
            case ExternalResultDescription::ValueType::vtUInt8:
                expected = CASS_VALUE_TYPE_TINY_INT;
                expected_text = "tinyint";
                break;
            case ExternalResultDescription::ValueType::vtInt16:
            case ExternalResultDescription::ValueType::vtUInt16:
                expected = CASS_VALUE_TYPE_SMALL_INT;
                expected_text = "smallint";
                break;
            case ExternalResultDescription::ValueType::vtUInt32:
            case ExternalResultDescription::ValueType::vtInt32:
                expected = CASS_VALUE_TYPE_INT;
                expected_text = "int";
                break;
            case ExternalResultDescription::ValueType::vtInt64:
            case ExternalResultDescription::ValueType::vtUInt64:
                expected = CASS_VALUE_TYPE_BIGINT;
                expected_text = "bigint";
                break;
            case ExternalResultDescription::ValueType::vtFloat32:
                expected = CASS_VALUE_TYPE_FLOAT;
                expected_text = "float";
                break;
            case ExternalResultDescription::ValueType::vtFloat64:
                expected = CASS_VALUE_TYPE_DOUBLE;
                expected_text = "double";
                break;
            case ExternalResultDescription::ValueType::vtString:
                expected = CASS_VALUE_TYPE_TEXT;
                expected_text = "text, ascii or varchar";
                break;
            case ExternalResultDescription::ValueType::vtDate:
                expected = CASS_VALUE_TYPE_DATE;
                expected_text = "date";
                break;
            case ExternalResultDescription::ValueType::vtDateTime:
                expected = CASS_VALUE_TYPE_TIMESTAMP;
                expected_text = "timestamp";
                break;
            case ExternalResultDescription::ValueType::vtUUID:
                expected = CASS_VALUE_TYPE_UUID;
                expected_text = "uuid";
                break;
        }

        CassValueType got = cass_result_column_type(result, i);

        if (got != expected)
        {
            if (expected == CASS_VALUE_TYPE_TEXT && (got == CASS_VALUE_TYPE_ASCII || got == CASS_VALUE_TYPE_VARCHAR))
                continue;

            const auto & column_name = description.sample_block.getColumnsWithTypeAndName()[i].name;
            throw Exception("Type mismatch for column " + column_name + ": expected Cassandra type " + expected_text,
                            ErrorCodes::TYPE_MISMATCH);
        }
    }

    assert_types = false;
}

}
#endif
