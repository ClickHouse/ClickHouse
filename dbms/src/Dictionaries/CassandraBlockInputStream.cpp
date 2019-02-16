#include <Common/config.h>
#include <Columns/ColumnsNumber.h>

#if USE_CASSANDRA

#   include "CassandraBlockInputStream.h"
#include "CassandraBlockInputStream.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int CASSANDRA_INTERNAL_ERROR;
}

CassandraBlockInputStream::CassandraBlockInputStream(
    CassSession *session,
    const std::string &query_str,
    const DB::Block &sample_block,
    const size_t max_block_size)
    : session{session}
    , query_str{query_str}
    , max_block_size{max_block_size}
{
    CassStatement * statement = cass_statement_new(query_str.c_str(), 0);
    CassFuture* future = cass_session_execute(session, statement);

    const CassResult * result = cass_future_get_result(future);
    cass_statement_free(statement);

    if (result == nullptr) {
//        CassError error_code = cass_future_error_code(future);
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(future, &error_message, &error_message_length);

        throw Exception{error_message, ErrorCodes::CASSANDRA_INTERNAL_ERROR};
    }

    cass_future_free(future);

    this->result = result;

    description.init(sample_block);
}

CassandraBlockInputStream::~CassandraBlockInputStream() {
    if (iterator != nullptr)
        cass_iterator_free(iterator);
    cass_result_free(result);
}

namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    void insertValue(IColumn & column, const ValueType type, const CassValue * value)
    {
        switch (type)
        {
            case ValueType::UInt8:
            {
                cass_uint32_t _value;
                cass_value_get_uint32(value, &_value);
                static_cast<ColumnUInt8 &>(column).insertValue(_value);
                break;
            }
            case ValueType::UInt16:
            {
                cass_uint32_t _value;
                cass_value_get_uint32(value, &_value);
                static_cast<ColumnUInt16 &>(column).insertValue(_value);
                break;
            }
            case ValueType::UInt32:
            {
                cass_uint32_t _value;
                cass_value_get_uint32(value, &_value);
                static_cast<ColumnUInt32 &>(column).insertValue(_value);
                break;
            }
            case ValueType::UInt64:
            {
                cass_int64_t _value;
                cass_value_get_int64(value, &_value);
                static_cast<ColumnUInt64 &>(column).insertValue(_value);
                break;
            }
            case ValueType::Int8:
            {
                cass_int8_t _value;
                cass_value_get_int8(value, &_value);
                static_cast<ColumnInt8 &>(column).insertValue(_value);
                break;
            }
            case ValueType::Int16:
            {
                cass_int16_t _value;
                cass_value_get_int16(value, &_value);
                static_cast<ColumnInt16 &>(column).insertValue(_value);
                break;
            }
            case ValueType::Int32:
            {
                cass_int32_t _value;
                cass_value_get_int32(value, &_value);
                static_cast<ColumnInt32 &>(column).insertValue(_value);
                break;
            }
            case ValueType::Int64:
            {
                cass_int64_t _value;
                cass_value_get_int64(value, &_value);
                static_cast<ColumnInt64 &>(column).insertValue(_value);
                break;
            }
            case ValueType::Float32:
            {
                cass_float_t _value;
                cass_value_get_float(value, &_value);
                static_cast<ColumnFloat32 &>(column).insertValue(_value);
                break;
            }
            case ValueType::Float64:
            {
                cass_double_t _value;
                cass_value_get_double(value, &_value);
                static_cast<ColumnFloat64 &>(column).insertValue(_value);
                break;
            }
            case ValueType::String:
            {
                const char* _value;
                size_t _value_length;
                cass_value_get_string
                static_cast<ColumnString &>(column).insertData(value.data(), value.size());
                break;
            }
            case ValueType::Date:
                static_cast<ColumnUInt16 &>(column).insertValue(UInt16(value.getDate().getDayNum()));
                break;
            case ValueType::DateTime:
                static_cast<ColumnUInt32 &>(column).insertValue(UInt32(value.getDateTime()));
                break;
            case ValueType::UUID:
                static_cast<ColumnUInt128 &>(column).insert(parse<UUID>(value.data(), value.size()));
                break;
        }
    }

    void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }
}


}
#endif
