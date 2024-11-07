#include "RedisSource.h"

#include <vector>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int TYPE_MISMATCH;
        extern const int LOGICAL_ERROR;
        extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
        extern const int INTERNAL_REDIS_ERROR;
        extern const int UNKNOWN_TYPE;
    }


    RedisSource::RedisSource(
        RedisConnectionPtr connection_,
        const RedisArray & keys_,
        const RedisStorageType & storage_type_,
        const DB::Block & sample_block,
        size_t max_block_size_)
        : ISource(sample_block)
        , connection(std::move(connection_))
        , keys(keys_)
        , storage_type(storage_type_)
        , max_block_size{max_block_size_}
    {
        description.init(sample_block);
    }

    RedisSource::~RedisSource() = default;

    namespace
    {
        using ValueType = ExternalResultDescription::ValueType;

        template <typename T>
        inline void insert(IColumn & column, const String & string_value)
        {
            assert_cast<ColumnVector<T> &>(column).insertValue(parse<T>(string_value));
        }

        void insertValue(IColumn & column, const ValueType type, const Poco::Redis::BulkString & bulk_string)
        {
            if (bulk_string.isNull())
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected not Null String");

            const String & string_value = bulk_string.value();
            switch (type)
            {
                case ValueType::vtUInt8:
                    insert<UInt8>(column, string_value);
                    break;
                case ValueType::vtUInt16:
                    insert<UInt16>(column, string_value);
                    break;
                case ValueType::vtUInt32:
                    insert<UInt32>(column, string_value);
                    break;
                case ValueType::vtUInt64:
                    insert<UInt64>(column, string_value);
                    break;
                case ValueType::vtInt8:
                    insert<Int8>(column, string_value);
                    break;
                case ValueType::vtInt16:
                    insert<Int16>(column, string_value);
                    break;
                case ValueType::vtInt32:
                    insert<Int32>(column, string_value);
                    break;
                case ValueType::vtInt64:
                    insert<Int64>(column, string_value);
                    break;
                case ValueType::vtFloat32:
                    insert<Float32>(column, string_value);
                    break;
                case ValueType::vtFloat64:
                    insert<Float64>(column, string_value);
                    break;
                case ValueType::vtEnum8:
                case ValueType::vtEnum16:
                case ValueType::vtString:
                    assert_cast<ColumnString &>(column).insert(parse<String>(string_value));
                    break;
                case ValueType::vtDate:
                    assert_cast<ColumnUInt16 &>(column).insertValue(parse<LocalDate>(string_value).getDayNum());
                    break;
                case ValueType::vtDateTime:
                {
                    ReadBufferFromString in(string_value);
                    time_t time = 0;
                    readDateTimeText(time, in);
                    time = std::max<time_t>(time, 0);
                    assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(time));
                    break;
                }
                case ValueType::vtUUID:
                    assert_cast<ColumnUUID &>(column).insertValue(parse<UUID>(string_value));
                    break;
                default:
                    throw Exception(ErrorCodes::UNKNOWN_TYPE,
                        "Value of unsupported type: {}",
                        column.getName());
            }
        }
    }

    Chunk RedisSource::generate()
    {
        if (keys.isNull() || description.sample_block.rows() == 0 || cursor >= keys.size())
            all_read = true;

        if (all_read)
            return {};

        const size_t size = description.sample_block.columns();
        MutableColumns columns(size);

        for (size_t i = 0; i < size; ++i)
            columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

        const auto insert_value_by_idx = [this, &columns](size_t idx, const auto & value)
        {
            if (description.types[idx].second)
            {
                ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*columns[idx]);
                insertValue(column_nullable.getNestedColumn(), description.types[idx].first, value);
                column_nullable.getNullMapData().emplace_back(0);
            }
            else
                insertValue(*columns[idx], description.types[idx].first, value);
        };

        if (storage_type == RedisStorageType::HASH_MAP)
        {
            size_t num_rows = 0;
            for (; cursor < keys.size(); ++cursor)
            {
                const auto & keys_array = keys.get<RedisArray>(cursor);
                if (keys_array.size() < 2)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Too low keys in request to source: {}, expected 2 or more",
                        keys_array.size());
                }

                if (num_rows + keys_array.size() - 1 > max_block_size)
                    break;

                Poco::Redis::Command command_for_values("HMGET");
                for (const auto & elem : keys_array)
                    command_for_values.addRedisType(elem);

                auto values = connection->client->execute<RedisArray>(command_for_values);

                if (keys_array.size() != values.size() + 1) // 'HMGET' primary_key secondary_keys
                    throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
                        "Inconsistent sizes of keys and values in Redis request");

                const auto & primary_key = keys_array.get<RedisBulkString>(0);
                for (size_t i = 0; i < values.size(); ++i)
                {
                    const auto & secondary_key = keys_array.get<RedisBulkString>(i + 1);
                    const auto & value = values.get<RedisBulkString>(i);

                    /// null string means 'no value for requested key'
                    if (!value.isNull())
                    {
                        insert_value_by_idx(0, primary_key);
                        insert_value_by_idx(1, secondary_key);
                        insert_value_by_idx(2, value);
                        ++num_rows;
                    }
                }
            }
        }
        else
        {
            Poco::Redis::Command command_for_values("MGET");

            size_t need_values = std::min(max_block_size, keys.size() - cursor);
            for (size_t i = 0; i < need_values; ++i)
                command_for_values.add(keys.get<RedisBulkString>(cursor + i));

            auto values = connection->client->execute<RedisArray>(command_for_values);
            if (values.size() != need_values)
                throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                    "Inconsistent sizes of keys and values in Redis request");

            for (size_t i = 0; i < values.size(); ++i)
            {
                const auto & key = keys.get<RedisBulkString>(cursor + i);
                const auto & value = values.get<RedisBulkString>(i);

                /// Null string means 'no value for requested key'
                if (!value.isNull())
                {
                    insert_value_by_idx(0, key);
                    insert_value_by_idx(1, value);
                }
            }
            cursor += need_values;
        }

        size_t num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }
}
