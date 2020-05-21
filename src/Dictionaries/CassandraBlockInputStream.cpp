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
    CassSession *session_,
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

    void insertValue(IColumn & column, const ValueType type, const CassValue * value)
    {
        switch (type)
        {
            case ValueType::vtUInt8:
            {
                cass_uint32_t _value;
                cass_value_get_uint32(value, &_value);
                static_cast<ColumnUInt8 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtUInt16:
            {
                cass_uint32_t _value;
                cass_value_get_uint32(value, &_value);
                static_cast<ColumnUInt16 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtUInt32:
            {
                cass_uint32_t _value;
                cass_value_get_uint32(value, &_value);
                static_cast<ColumnUInt32 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtUInt64:
            {
                cass_int64_t _value;
                cass_value_get_int64(value, &_value);
                static_cast<ColumnUInt64 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtInt8:
            {
                cass_int8_t _value;
                cass_value_get_int8(value, &_value);
                static_cast<ColumnInt8 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtInt16:
            {
                cass_int16_t _value;
                cass_value_get_int16(value, &_value);
                static_cast<ColumnInt16 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtInt32:
            {
                cass_int32_t _value;
                cass_value_get_int32(value, &_value);
                static_cast<ColumnInt32 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtInt64:
            {
                cass_int64_t _value;
                cass_value_get_int64(value, &_value);
                static_cast<ColumnInt64 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtFloat32:
            {
                cass_float_t _value;
                cass_value_get_float(value, &_value);
                static_cast<ColumnFloat32 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtFloat64:
            {
                cass_double_t _value;
                cass_value_get_double(value, &_value);
                static_cast<ColumnFloat64 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtString:
            {
                const char* _value;
                size_t _value_length;
                cass_value_get_string(value, &_value, &_value_length);
                static_cast<ColumnString &>(column).insertData(_value, _value_length);
                break;
            }
            case ValueType::vtDate:
            {
                cass_int64_t _value;
                cass_value_get_int64(value, &_value);
                static_cast<ColumnUInt16 &>(column).insertValue(UInt32{cass_date_from_epoch(_value)}); // FIXME
                break;
            }
            case ValueType::vtDateTime:
            {
                cass_int64_t _value;
                cass_value_get_int64(value, &_value);
                static_cast<ColumnUInt32 &>(column).insertValue(_value);
                break;
            }
            case ValueType::vtUUID:
            {
                CassUuid _value;
                cass_value_get_uuid(value, &_value);
                std::array<char, CASS_UUID_STRING_LENGTH> uuid_str;
                cass_uuid_string(_value, uuid_str.data());
                static_cast<ColumnUInt128 &>(column).insert(parse<UUID>(uuid_str.data(), uuid_str.size()));
                break;
            }
        }
    }
}

    // void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }

    Block CassandraBlockInputStream::readImpl()
    {
        if (has_more_pages)
            return {};

        MutableColumns columns(description.sample_block.columns());
        CassFuture* query_future = cass_session_execute(session, statement);

        result = cass_future_get_result(query_future);

        if (result == nullptr) {
            const char* error_message;
            size_t error_message_length;
            cass_future_error_message(query_future, &error_message, &error_message_length);

            throw Exception{error_message, ErrorCodes::CASSANDRA_INTERNAL_ERROR};
        }

        const CassRow* row = cass_result_first_row(result);
        const CassValue* map = cass_row_get_column(row, 0);
        iterator = cass_iterator_from_map(map);
        while (cass_iterator_next(iterator)) {
            const CassValue* _key = cass_iterator_get_map_key(iterator);
            const CassValue* _value = cass_iterator_get_map_value(iterator);
            auto pair_values = {std::make_pair(_key, 0ul), std::make_pair(_value, 1ul)};
            for (const auto &[value, idx]: pair_values) {
                if (description.types[idx].second) {
                    ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*columns[idx]);
                    insertValue(column_nullable.getNestedColumn(), description.types[idx].first, value);
                    column_nullable.getNullMapData().emplace_back(0);
                } else {
                    insertValue(*columns[idx], description.types[idx].first, value);
                }
            }
        }

        has_more_pages = cass_result_has_more_pages(result);

        if (has_more_pages) {
            cass_statement_set_paging_state(statement, result);
        }

        cass_result_free(result);

        return description.sample_block.cloneWithColumns(std::move(columns));
    }


}
#endif
