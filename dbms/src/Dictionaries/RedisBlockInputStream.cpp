#include <Common/config.h>
#if USE_POCO_REDIS

#    include <sstream>
#    include <string>
#    include <vector>

#    include <Poco/Redis/Array.h>
#    include <Poco/Redis/AsyncReader.h>
#    include <Poco/Redis/Client.h>
#    include <Poco/Redis/Command.h>
#    include <Poco/Redis/Error.h>
#    include <Poco/Redis/Exception.h>
#    include <Poco/Redis/RedisEventArgs.h>
#    include <Poco/Redis/RedisStream.h>
#    include <Poco/Redis/Type.h>

#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsNumber.h>
#    include <IO/ReadHelpers.h>
#    include <IO/WriteHelpers.h>
#    include <Common/FieldVisitors.h>
#    include <ext/range.h>
#    include "DictionaryStructure.h"
#    include "RedisBlockInputStream.h"

#    include "Poco/Logger.h"
#    include "common/logger_useful.h"


namespace DB
{
    namespace ErrorCodes
    {
        extern const int TYPE_MISMATCH;
        extern const int LOGICAL_ERROR;
        extern const int LIMIT_EXCEEDED;
    }


    RedisBlockInputStream::RedisBlockInputStream(
            const std::shared_ptr<Poco::Redis::Client> & client_,
            const Poco::Redis::Array & keys_,
            const DB::Block & sample_block,
            const size_t max_block_size)
            : client(client_), keys(keys_), max_block_size{max_block_size}
    {
        description.init(sample_block);
    }

    RedisBlockInputStream::~RedisBlockInputStream() = default;


    namespace
    {
        using ValueType = ExternalResultDescription::ValueType;
        using RedisArray = Poco::Redis::Array;

        template <typename T>
        void insertNumber(IColumn & column, const Poco::Redis::RedisType::Ptr & value, const std::string & name)
        {
            LOG_ERROR(&Logger::get("Redis"), "Got value: " + value->toString() + "with type=" +
                ", isInteger=" + DB::toString(value->isInteger()) +
                ", isSimpleString=" + DB::toString(value->isSimpleString()) +
                ", isBulkString=" + DB::toString(value->isBulkString()) +
                ", isArray=" + DB::toString(value->isArray()) +
                ", isError=" + DB::toString(value->isError()));
            switch (value->type())
            {
                case Poco::Redis::RedisTypeTraits<Poco::Int64>::TypeId:
                    static_cast<ColumnVector<T> &>(column).getData().push_back(
                            static_cast<const Poco::Redis::Type<Poco::Int64> *>(value.get())->value());
                    break;
                case Poco::Redis::RedisTypeTraits<std::string>::TypeId:
                    static_cast<ColumnVector<T> &>(column).getData().push_back(
                            parse<T>(static_cast<const Poco::Redis::Type<std::string> *>(value.get())->value()));
                    break;
                case Poco::Redis::RedisTypeTraits<Poco::Redis::BulkString>::TypeId:
                {
                    const auto & bs =
                            static_cast<const Poco::Redis::Type<Poco::Redis::BulkString> *>(value.get())->value();
                    if (bs.isNull())
                        static_cast<ColumnVector<T> &>(column).getData().emplace_back();
                    else
                        static_cast<ColumnVector<T> &>(column).getData().push_back(parse<T>(bs.value()));
                    break;
                }
                default:
                    throw Exception(
                            "Type mismatch, expected a number, got " + value->toString() +
                            " with type id = " + toString(value->type()) + " for column " + name,
                            ErrorCodes::TYPE_MISMATCH);
            }
        }

        void insertValue(IColumn & column, const ValueType type, const Poco::Redis::RedisType::Ptr & value, const std::string & name)
        {
            auto getStringIfCould = [&value, &name]()
            {
                switch (value->type())
                {
                    case Poco::Redis::RedisTypeTraits<Poco::Redis::BulkString>::TypeId:
                    {
                        const auto & bs = static_cast<const Poco::Redis::Type<Poco::Redis::BulkString> *>(value.get())->value();
                        if (bs.isNull())
                            throw Exception{"Type mismatch, expected not null String for column " + name,
                                            ErrorCodes::TYPE_MISMATCH};
                        return bs.value();
                    }
                    case Poco::Redis::RedisTypeTraits<std::string>::TypeId:
                        return static_cast<const Poco::Redis::Type<std::string> *>(value.get())->value();
                    default:
                        throw Exception{"Type mismatch, expected std::string, got type id = " + toString(value->type()) + " for column " + name,
                                        ErrorCodes::TYPE_MISMATCH};
                }
            };

            auto getInt64IfCould = [&value]()
            {
                switch (value->type())
                {
                    case Poco::Redis::RedisTypeTraits<Int64>::TypeId:
                    {
                        return static_cast<const Poco::Redis::Type<Poco::Int64> *>(value.get())->value();
                    }
                    case Poco::Redis::RedisTypeTraits<std::string>::TypeId:
                    {
                        return parse<Int64>(
                                static_cast<const Poco::Redis::Type<std::string> *>(value.get())->value());
                    }
                    case Poco::Redis::RedisTypeTraits<Poco::Redis::BulkString>::TypeId:
                    {
                        const auto & bs = static_cast<const Poco::Redis::Type<Poco::Redis::BulkString> *>(
                                value.get())->value();
                        if (bs.isNull())
                            throw Exception{"Unexpected null value", ErrorCodes::TYPE_MISMATCH};
                        return parse<Int64>(bs.value());
                    }
                    default:
                    {
                        throw Exception{"Type mismatch, cannot convert to Int64, got type id = " + toString(value->type()),
                                        ErrorCodes::TYPE_MISMATCH};
                    }
                }
            };

            switch (type)
            {
                case ValueType::UInt8:
                    insertNumber<UInt8>(column, value, name);
                    break;
                case ValueType::UInt16:
                    insertNumber<UInt16>(column, value, name);
                    break;
                case ValueType::UInt32:
                    insertNumber<UInt32>(column, value, name);
                    break;
                case ValueType::UInt64:
                    insertNumber<UInt64>(column, value, name);
                    break;
                case ValueType::Int8:
                    insertNumber<Int8>(column, value, name);
                    break;
                case ValueType::Int16:
                    insertNumber<Int16>(column, value, name);
                    break;
                case ValueType::Int32:
                    insertNumber<Int32>(column, value, name);
                    break;
                case ValueType::Int64:
                    insertNumber<Int64>(column, value, name);
                    break;
                case ValueType::Float32:
                    insertNumber<Float32>(column, value, name);
                    break;
                case ValueType::Float64:
                    insertNumber<Float64>(column, value, name);
                    break;

                case ValueType::String:
                {
                    String string = getStringIfCould();
                    static_cast<ColumnString &>(column).insertDataWithTerminatingZero(string.data(), string.size() + 1);
                    break;
                }

                case ValueType::Date:
                {
                    Int64 int_value = getInt64IfCould();
                    static_cast<ColumnUInt16 &>(column).getData().push_back(UInt16{DateLUT::instance().toDayNum(
                            static_cast<const Poco::Timestamp &>(int_value).epochTime())});
                    break;
                }

                case ValueType::DateTime:
                {
                    Int64 int_value = getInt64IfCould();
                    static_cast<ColumnUInt32 &>(column).getData().push_back(
                            static_cast<const Poco::Timestamp &>(int_value).epochTime());
                    break;
                }
                case ValueType::UUID:
                {
                    String string = getStringIfCould();
                    static_cast<ColumnUInt128 &>(column).getData().push_back(parse<UUID>(string));
                    break;
                }
            }
        }

        void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }
    }


    Block RedisBlockInputStream::readImpl()
    {
        if (description.sample_block.rows() == 0 || keys.size() == 0)
            all_read = true;

        if (all_read)
            return {};

        for (size_t i = 0; i < 5; ++i)
            if (description.sample_block.columns() >= i + 1)
                LOG_ERROR(&Logger::get("Redis"), description.sample_block.getByPosition(i).dumpStructure());

        const size_t size = description.sample_block.columns();
//        const size_t size = 2;
//        if (size != description.sample_block.columns())
//            throw Exception{"Unsupported number of columns for key-value storage: "
//                            + DB::toString(description.sample_block.columns())
//                            + " (expected: " + DB::toString(size) + ")",
//                            ErrorCodes::LOGICAL_ERROR};

        MutableColumns columns(description.sample_block.columns());

        for (const auto i : ext::range(0, size))
            columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

        const auto insertValueByIdx = [this, &columns](size_t idx, const auto & value)
        {
            const auto & name = description.sample_block.getByPosition(idx).name;
            if (description.types[idx].second)
            {
                ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*columns[idx]);
                insertValue(column_nullable.getNestedColumn(), description.types[idx].first, value, name);
                column_nullable.getNullMapData().emplace_back(0);
            }
            else
                insertValue(*columns[idx], description.types[idx].first, value, name);
        };

        if (keys.begin()->get()->isArray())
        {
            size_t num_rows = 0;
            while (num_rows < max_block_size)
            {
                if (cursor >= keys.size())
                {
                    all_read = true;
                    break;
                }

                const auto & primary_with_secondary = *(keys.begin() + cursor);
                const auto & keys_array =
                        static_cast<const Poco::Redis::Type<Poco::Redis::Array> *>(primary_with_secondary.get())->value();
                if (keys_array.size() < 2)
                {
                    throw Exception{"Too low keys in request to source: " + DB::toString(keys_array.size())
                                    + ", expected 2 or more",
                                    ErrorCodes::LOGICAL_ERROR};
                }
                if (num_rows + keys_array.size() - 1 > max_block_size)
                {
                    if (num_rows == 0)
                        throw Exception{"Too many (" + DB::toString(keys_array.size()) + ") key attributes",
                                        ErrorCodes::LIMIT_EXCEEDED};
                    break;
                }

                Poco::Redis::Command commandForValues("HMGET");
                const auto & primary_key = *keys_array.begin();
                for (size_t i = 1; i < keys_array.size(); ++i)
                {
                    const auto & secondary_key = *(keys_array.begin() + i);
                    insertValueByIdx(0, primary_key);
                    insertValueByIdx(1, secondary_key);
                    commandForValues.addRedisType(secondary_key);
                }

                Poco::Redis::Array values = client->execute<Poco::Redis::Array>(commandForValues);
                for (const auto & value : values)
                {
                    if (value.isNull())
                        insertDefaultValue(*columns[2], *description.sample_block.getByPosition(2).column);
                    else
                        insertValueByIdx(2, value);
                }

                num_rows += keys_array.size() - 1;
                cursor += keys_array.size() - 1;
            }
        }
        else
        {
            size_t num_rows = 0;
            Poco::Redis::Command commandForValues("MGET");

            while (num_rows < max_block_size)
            {
                if (cursor >= keys.size())
                {
                    all_read = true;
                    break;
                }

                const auto & key = *(keys.begin() + cursor);
                insertValueByIdx(0, key);
                commandForValues.addRedisType(key);

                ++num_rows;
                ++cursor;
            }

            if (num_rows == 0)
                return {};

            Poco::Redis::Array values = client->execute<Poco::Redis::Array>(commandForValues);
            for (const auto & value : values)
            {
                if (value.isNull())
                    insertDefaultValue(*columns[1], *description.sample_block.getByPosition(1).column);
                else
                    insertValueByIdx(1, value);
            }
        }

        return description.sample_block.cloneWithColumns(std::move(columns));
    }
}

#endif
