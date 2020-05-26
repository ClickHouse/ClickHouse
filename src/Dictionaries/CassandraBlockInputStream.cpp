#    include <Common/config.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsNumber.h>
#    include <Core/ExternalResultDescription.h>
#    include <Columns/ColumnString.h>
#    include <IO/ReadHelpers.h>

#if USE_CASSANDRA

#   include <utility>
#   include "CassandraBlockInputStream.h"
#   include "CassandraBlockInputStream.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int CASSANDRA_INTERNAL_ERROR;
}

CassandraBlockInputStream::CassandraBlockInputStream(
    CassSession * session_,
    const std::string &query_str_,
    const DB::Block &sample_block,
    const size_t max_block_size_)
    : session(session_)
    , statement(cass_statement_new(query_str_.c_str(), 0))
    , query_str(query_str_)
    , max_block_size(max_block_size_)
{
    cass_statement_set_paging_size(statement, max_block_size);
    this->has_more_pages = cass_true;

    description.init(sample_block);
}

CassandraBlockInputStream::~CassandraBlockInputStream() {
    if (iterator != nullptr)
        cass_iterator_free(iterator);
    if (result)
        cass_result_free(result);
}

namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    void insertValue(IColumn & column, const ValueType type, const CassValue * cass_value)
    {
        /// Cassandra does not support unsigned integers
        switch (type)
        {
            case ValueType::vtUInt8:
            {
                cass_int8_t value;
                cass_value_get_int8(cass_value, &value);
                assert_cast<ColumnUInt8 &>(column).insertValue(value);
                break;
            }
            case ValueType::vtUInt16:
            {
                cass_int16_t value;
                cass_value_get_int16(cass_value, &value);
                assert_cast<ColumnUInt16 &>(column).insertValue(value);
                break;
            }
            case ValueType::vtUInt32:
            {
                cass_int32_t value;
                cass_value_get_int32(cass_value, &value);
                assert_cast<ColumnUInt32 &>(column).insertValue(value);
                break;
            }
            case ValueType::vtUInt64:
            {
                cass_int64_t value;
                cass_value_get_int64(cass_value, &value);
                assert_cast<ColumnUInt64 &>(column).insertValue(value);
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
                const char * value;
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
}

    // void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }

    Block CassandraBlockInputStream::readImpl()
    {
        if (!has_more_pages)
            return {};

        MutableColumns columns = description.sample_block.cloneEmptyColumns();
        CassFuture* query_future = cass_session_execute(session, statement);

        result = cass_future_get_result(query_future);

        if (result == nullptr) {
            const char* error_message;
            size_t error_message_length;
            cass_future_error_message(query_future, &error_message, &error_message_length);

            throw Exception{error_message, ErrorCodes::CASSANDRA_INTERNAL_ERROR};
        }

        [[maybe_unused]] size_t row_count = 0;
        assert(cass_result_column_count(result) == columns.size());
        CassIterator * rows_iter = cass_iterator_from_result(result);     /// Points to rows[-1]
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
                    ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*columns[col_idx]);
                    insertValue(column_nullable.getNestedColumn(), description.types[col_idx].first, val);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                    insertValue(*columns[col_idx], description.types[col_idx].first, val);
            }
            ++row_count;
        }
        assert(cass_result_row_count(result) == row_count);
        cass_iterator_free(rows_iter);

        //const CassRow* row = cass_result_first_row(result);
        //const CassValue* map = cass_row_get_column(row, 0);
        //const CassValue* map = cass_row_get_column(row, 0);
        //iterator = cass_iterator_from_map(map);
        //while (cass_iterator_next(iterator)) {
        //    const CassValue* cass_key = cass_iterator_get_map_key(iterator);
        //    const CassValue* cass_value = cass_iterator_get_map_value(iterator);
        //    auto pair_values = {std::make_pair(cass_key, 0ul), std::make_pair(cass_value, 1ul)};
        //    for (const auto &[value, idx]: pair_values) {
        //        if (description.types[idx].second) {
        //            ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*columns[idx]);
        //            insertValue(column_nullable.getNestedColumn(), description.types[idx].first, value);
        //            column_nullable.getNullMapData().emplace_back(0);
        //        } else {
        //            insertValue(*columns[idx], description.types[idx].first, value);
        //        }
        //    }
        //}

        has_more_pages = cass_result_has_more_pages(result);

        if (has_more_pages) {
            cass_statement_set_paging_state(statement, result);
        }

        cass_result_free(result);

        return description.sample_block.cloneWithColumns(std::move(columns));
    }


void cassandraCheck(CassError code)
{
    if (code != CASS_OK)
        throw Exception("Cassandra driver error " + std::to_string(code) + ": " + cass_error_desc(code),
                        ErrorCodes::CASSANDRA_INTERNAL_ERROR);
}

void cassandraWaitAndCheck(CassFuture * future)
{
    auto code = cass_future_error_code(future);     /// Waits if not ready
    if (code == CASS_OK)
    {
        cass_future_free(future);
        return;
    }
    const char * message;
    size_t message_len;
    cass_future_error_message(future, &message, & message_len);
    String full_message = "Cassandra driver error " + std::to_string(code) + ": " + cass_error_desc(code) + ": " + message;
    cass_future_free(future);       /// Frees message
    throw Exception(full_message, ErrorCodes::CASSANDRA_INTERNAL_ERROR);
}

}
#endif
