#include <Server/RedisHandler.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Interpreters/StorageID.h>
#include <Processors/Chunk.h>
#include <Server/IServer.h>
#include <Server/RedisProtocol.h>
#include <Server/TCPServer.h>
#include <Storages/IStorage.h>

#include <Poco/Util/LayeredConfiguration.h>

#include <charconv>
#include <memory>
#include <string_view>
#include <system_error>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

namespace
{

constexpr size_t MAX_KEY_SIZE = 64 * 1024;
constexpr size_t MAX_MGET_KEYS = 1024;

bool parseRedisUInt64Key(const String & key, UInt64 & value)
{
    if (key.empty() || key.front() < '0' || key.front() > '9')
        return false;

    const char * begin = key.data();
    const char * end = key.data() + key.size();
    auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
}

bool buildKeyColumns(
    TypeIndex key_type,
    const String & primary_key_name,
    const std::vector<String> & keys_to_get,
    ColumnsWithTypeAndName & keys,
    String & error)
{
    if (key_type == TypeIndex::String)
    {
        auto key_column = ColumnString::create();
        for (const auto & key : keys_to_get)
            key_column->insertData(key.data(), key.size());

        keys.push_back({
            std::move(key_column),
            std::make_shared<DataTypeString>(),
            primary_key_name});
        return true;
    }

    if (key_type == TypeIndex::UInt64)
    {
        auto key_column = ColumnUInt64::create();
        for (const auto & key : keys_to_get)
        {
            UInt64 parsed_key = 0;
            if (!parseRedisUInt64Key(key, parsed_key))
            {
                error = "ERR invalid UInt64 key";
                return false;
            }
            key_column->insertValue(parsed_key);
        }

        keys.push_back({
            std::move(key_column),
            std::make_shared<DataTypeUInt64>(),
            primary_key_name});
        return true;
    }

    error = "ERR only String or UInt64 key column is supported";
    return false;
}

}

RedisHandler::RedisHandler(
    IServer & server_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    UInt64 connection_id_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(getLogger("RedisHandler"))
    , connection_id(connection_id_)
{
}

bool RedisHandler::selectDatabase(const String & db_index)
{
    if (db_index.empty())
        return false;

    UInt64 parsed_db = 0;
    const char * begin = db_index.data();
    const char * end = db_index.data() + db_index.size();
    auto result = std::from_chars(begin, end, parsed_db);
    if (result.ec != std::errc{} || result.ptr != end)
        return false;

    const auto & config = server.config();
    String prefix = "redis.db._" + std::to_string(parsed_db);

    TargetConfig target;
    target.database = config.getString(prefix + ".database", "");
    target.table = config.getString(prefix + ".table", "");
    target.default_column = config.getString(prefix + ".default_column", "");

    if (target.database.empty() || target.table.empty() || target.default_column.empty())
        return false;

    selected_db = parsed_db;
    selected_target = std::move(target);
    has_selected_target = true;
    return true;
}

void RedisHandler::getKey(WriteBuffer & out, const String & key)
{
    if (!has_selected_target)
    {
        RedisProtocol::writeError(out, "ERR no Redis DB selected");
        return;
    }
    if (key.size() > MAX_KEY_SIZE)
    {
        RedisProtocol::writeError(out, "ERR key is too large");
        return;
    }

    try
    {
        ContextPtr context = server.context();
        StoragePtr storage = DatabaseCatalog::instance().getTable(
            StorageID(selected_target.database, selected_target.table),
            context);

        auto key_value = std::dynamic_pointer_cast<IKeyValueEntity>(storage);
        if (!key_value)
        {
            RedisProtocol::writeError(out, "ERR target table does not support key-value lookup");
            return;
        }

        Names primary_keys = key_value->getPrimaryKey();
        if (primary_keys.size() != 1)
        {
            RedisProtocol::writeError(out, "ERR only single-column primary key is supported");
            return;
        }

        Names required_columns{selected_target.default_column};
        Block sample_block = key_value->getSampleBlock(required_columns);
        size_t key_pos = sample_block.getPositionByName(primary_keys.front());
        size_t value_pos = sample_block.getPositionByName(selected_target.default_column);
        const auto key_type = sample_block.getByPosition(key_pos).type->getTypeId();

        if (sample_block.getByPosition(value_pos).type->getTypeId() != TypeIndex::String)
        {
            RedisProtocol::writeError(out, "ERR only String value column is supported");
            return;
        }

        ColumnsWithTypeAndName keys;
        String key_error;
        if (!buildKeyColumns(key_type, primary_keys.front(), {key}, keys, key_error))
        {
            RedisProtocol::writeError(out, key_error);
            return;
        }

        PaddedPODArray<UInt8> found_map;
        IColumn::Offsets offsets;
        Chunk chunk = key_value->getByKeys(keys, required_columns, found_map, offsets);

        if (!offsets.empty() || chunk.getNumRows() != 1 || found_map.size() != 1 || chunk.getNumColumns() <= value_pos)
        {
            RedisProtocol::writeError(out, "ERR unexpected key-value lookup result");
            return;
        }

        if (!found_map[0])
        {
            RedisProtocol::writeNullBulkString(out);
            return;
        }

        const auto * value_column = typeid_cast<const ColumnString *>(chunk.getColumns()[value_pos].get());
        if (!value_column)
        {
            RedisProtocol::writeError(out, "ERR value column is not String");
            return;
        }

        std::string_view value = value_column->getDataAt(0);
        RedisProtocol::writeBulkString(out, value);
    }
    catch (const Exception & e)
    {
        LOG_TRACE(
            log,
            "Redis GET failed. Id: {}. Target: {}.{}. Column: {}. Error: {}",
            connection_id,
            selected_target.database,
            selected_target.table,
            selected_target.default_column,
            e.message());
        RedisProtocol::writeError(out, "ERR " + e.message());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Redis GET failed with unexpected exception");
        RedisProtocol::writeError(out, "ERR failed to execute GET");
    }
}

void RedisHandler::getKeys(WriteBuffer & out, const std::vector<String> & keys_to_get)
{
    if (!has_selected_target)
    {
        RedisProtocol::writeError(out, "ERR no Redis DB selected");
        return;
    }
    if (keys_to_get.size() > MAX_MGET_KEYS)
    {
        RedisProtocol::writeError(out, "ERR too many keys for 'mget' command");
        return;
    }
    for (const auto & key : keys_to_get)
    {
        if (key.size() > MAX_KEY_SIZE)
        {
            RedisProtocol::writeError(out, "ERR key is too large");
            return;
        }
    }

    try
    {
        ContextPtr context = server.context();
        StoragePtr storage = DatabaseCatalog::instance().getTable(
            StorageID(selected_target.database, selected_target.table),
            context);

        auto key_value = std::dynamic_pointer_cast<IKeyValueEntity>(storage);
        if (!key_value)
        {
            RedisProtocol::writeError(out, "ERR target table does not support key-value lookup");
            return;
        }

        Names primary_keys = key_value->getPrimaryKey();
        if (primary_keys.size() != 1)
        {
            RedisProtocol::writeError(out, "ERR only single-column primary key is supported");
            return;
        }

        Names required_columns{selected_target.default_column};
        Block sample_block = key_value->getSampleBlock(required_columns);
        size_t key_pos = sample_block.getPositionByName(primary_keys.front());
        size_t value_pos = sample_block.getPositionByName(selected_target.default_column);
        const auto key_type = sample_block.getByPosition(key_pos).type->getTypeId();

        if (sample_block.getByPosition(value_pos).type->getTypeId() != TypeIndex::String)
        {
            RedisProtocol::writeError(out, "ERR only String value column is supported");
            return;
        }

        ColumnsWithTypeAndName keys;
        String key_error;
        if (!buildKeyColumns(key_type, primary_keys.front(), keys_to_get, keys, key_error))
        {
            RedisProtocol::writeError(out, key_error);
            return;
        }

        PaddedPODArray<UInt8> found_map;
        IColumn::Offsets offsets;
        Chunk chunk = key_value->getByKeys(keys, required_columns, found_map, offsets);

        const size_t requested_keys = keys_to_get.size();
        if (!offsets.empty() || chunk.getNumRows() != requested_keys || found_map.size() != requested_keys || chunk.getNumColumns() <= value_pos)
        {
            RedisProtocol::writeError(out, "ERR unexpected key-value lookup result");
            return;
        }

        const auto * value_column = typeid_cast<const ColumnString *>(chunk.getColumns()[value_pos].get());
        if (!value_column)
        {
            RedisProtocol::writeError(out, "ERR value column is not String");
            return;
        }

        RedisProtocol::writeArrayHeader(out, requested_keys);
        for (size_t i = 0; i < requested_keys; ++i)
        {
            if (!found_map[i])
                RedisProtocol::writeNullBulkString(out);
            else
                RedisProtocol::writeBulkString(out, value_column->getDataAt(i));
        }
    }
    catch (const Exception & e)
    {
        LOG_TRACE(
            log,
            "Redis MGET failed. Id: {}. Target: {}.{}. Column: {}. Error: {}",
            connection_id,
            selected_target.database,
            selected_target.table,
            selected_target.default_column,
            e.message());
        RedisProtocol::writeError(out, "ERR " + e.message());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Redis MGET failed with unexpected exception");
        RedisProtocol::writeError(out, "ERR failed to execute MGET");
    }
}

void RedisHandler::run()
{
    LOG_TRACE(log, "Redis connection started. Id: {}. Address: {}", connection_id, socket().peerAddress().toString());

    auto in = std::make_unique<ReadBufferFromPocoSocket>(socket());
    auto out = std::make_unique<AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>>(socket());

    try
    {
        while (tcp_server.isOpen())
        {
            RedisProtocol::Command command = RedisProtocol::readCommand(*in);

            if (command.name == "PING")
            {
                if (command.arguments.empty())
                    RedisProtocol::writeSimpleString(*out, "PONG");
                else if (command.arguments.size() == 1)
                    RedisProtocol::writeBulkString(*out, command.arguments.front());
                else
                    RedisProtocol::writeError(*out, "ERR wrong number of arguments for 'ping' command");
            }
            else if (command.name == "QUIT")
            {
                RedisProtocol::writeSimpleString(*out, "OK");
                out->finalize();
                return;
            }
            else if (command.name == "SELECT")
            {
                if (command.arguments.size() != 1)
                {
                    RedisProtocol::writeError(*out, "ERR wrong number of arguments for 'select' command");
                }
                else if (command.arguments.front().empty() || command.arguments.front().front() < '0' || command.arguments.front().front() > '9')
                {
                    RedisProtocol::writeError(*out, "ERR invalid DB index");
                }
                else
                {
                    UInt64 parsed_db = 0;
                    const String & db_index = command.arguments.front();
                    auto result = std::from_chars(db_index.data(), db_index.data() + db_index.size(), parsed_db);
                    if (result.ec != std::errc{} || result.ptr != db_index.data() + db_index.size())
                    {
                        RedisProtocol::writeError(*out, "ERR invalid DB index");
                    }
                    else if (selectDatabase(db_index))
                    {
                        RedisProtocol::writeSimpleString(*out, "OK");
                    }
                    else
                    {
                        RedisProtocol::writeError(*out, "ERR Redis DB " + std::to_string(parsed_db) + " is not configured");
                    }
                }
            }
            else if (command.name == "GET")
            {
                if (command.arguments.size() != 1)
                    RedisProtocol::writeError(*out, "ERR wrong number of arguments for 'get' command");
                else
                    getKey(*out, command.arguments.front());
            }
            else if (command.name == "MGET")
            {
                if (command.arguments.empty())
                    RedisProtocol::writeError(*out, "ERR wrong number of arguments for 'mget' command");
                else
                    getKeys(*out, command.arguments);
            }
            else
            {
                RedisProtocol::writeError(*out, "ERR unknown command '" + command.name + "'");
            }

            out->next();
        }

        out->finalize();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED)
        {
            LOG_TRACE(log, "Redis protocol error. Id: {}. Error: {}", connection_id, e.message());
            try
            {
                RedisProtocol::writeError(*out, "ERR Protocol error");
                out->finalize();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to send Redis protocol error");
            }
            return;
        }

        LOG_TRACE(log, "Redis connection finished with exception. Id: {}. Error: {}", connection_id, e.message());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Redis connection finished with unexpected exception");
    }
}

}
