#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include <Core/Field.h>
#include <Core/iostream_debug_helpers.h>
#include <Storages/IKVStorage.h>
#include "RedisHandler.h"
#include "RedisProtocol.hpp"

#include <Columns/ColumnString.h>

#include <boost/algorithm/string/split.hpp>

#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <Poco/Exception.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/setThreadName.h>

#include <DataTypes/DataTypeString.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>

#if USE_SSL
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_DATABASE;
}

RedisHandler::RedisHandler(const Poco::Net::StreamSocket & socket_, IServer & server_, TCPServer & tcp_server_)
    : Poco::Net::TCPServerConnection(socket_), server(server_), tcp_server(tcp_server_)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
}

void RedisHandler::run()
{
    setThreadName("RedisHandler");
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::REDIS);
    SCOPE_EXIT({ session.reset(); });

    try
    {
        if (server.config().getBool("redis.enable_ssl", false))
        {
#if USE_SSL
            makeSecureConnection();
#else
            throw DB::Exception(
                "Can't use SSL for redis protocol, because ClickHouse was built without SSL library", DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
        }

        while (tcp_server.isOpen())
        {
            SCOPE_EXIT(out->next());
            RedisProtocol::BeginRequest req;
            req.deserialize(*in);
            if (req.getMethod() == "get")
            {
                RedisProtocol::MGetRequest get_req(req);
                get_req.deserialize(*in);
                LOG_DEBUG(log, "GET request for {} key", get_req.getKeys()[0]);

                if (!validateGetRequest())
                    continue;

                auto result = getValuesByKeysAndDefaultColumn(get_req.getKeys());
                RedisProtocol::BulkStringResponse resp(result[0]);
                resp.serialize(*out);
            }
            else if (req.getMethod() == "mget")
            {
                RedisProtocol::MGetRequest get_req(req);
                get_req.deserialize(*in);
                LOG_DEBUG(log, "MGET request for {} keys", std::to_string(get_req.getKeys().size()));

                if (!validateGetRequest())
                    continue;

                auto result = getValuesByKeysAndDefaultColumn(get_req.getKeys());
                RedisProtocol::ArrayResponse resp(result);
                resp.serialize(*out);
            }
            else if (req.getMethod() == "hget")
            {
                RedisProtocol::HMGetRequest get_req(req);
                get_req.deserialize(*in);
                LOG_DEBUG(log, "HGET request for {} key {} column", get_req.getKey(), get_req.getColumns()[0]);

                if (!validateGetRequest())
                    continue;

                if (!validateColumns(get_req.getColumns()))
                {
                    RedisProtocol::ErrorResponse resp(Poco::format("No such column: %s", column));
                    resp.serialize(*out);
                    continue;
                }

                auto result = getValuesByKeyAndColumns(get_req.getKey(), get_req.getColumns());
                RedisProtocol::BulkStringResponse resp(result[0]);
                resp.serialize(*out);
            }
            else if (req.getMethod() == "hmget")
            {
                RedisProtocol::HMGetRequest get_req(req);
                get_req.deserialize(*in);
                LOG_DEBUG(log, "HMGET request for {} key {} columns", get_req.getKey(), get_req.getColumns().size());

                if (!validateGetRequest())
                    continue;

                if (!validateColumns(get_req.getColumns()))
                {
                    RedisProtocol::ErrorResponse resp(Poco::format("No such column: %s", column));
                    resp.serialize(*out);
                    continue;
                }

                auto result = getValuesByKeyAndColumns(get_req.getKey(), get_req.getColumns());
                RedisProtocol::ArrayResponse resp(result);
                resp.serialize(*out);
            }
            else if (req.getMethod() == "auth")
            {
                RedisProtocol::AuthRequest auth_req(req);
                auth_req.deserialize(*in);

                String username = auth_req.getUsername();
                if (auth_req.getUsername().empty())
                    username = "default";

                LOG_DEBUG(log, "AUTH request for {} username", auth_req.getUsername());

                RedisProtocol::Writer writer(out.get());
                if (authenticated)
                    writer.writeSimpleString(RedisProtocol::Message::OK);

                bool success
                    = authentication_manager.authenticate(username, auth_req.getPassword(), *session, socket().address(), out.get());

                authenticated = success;
            }
            else if (req.getMethod() == "select")
            {
                RedisProtocol::SelectRequest select_req(req);
                select_req.deserialize(*in);
                LOG_DEBUG(log, "SELECT request for {} db", std::to_string(select_req.getDb()));

                if (!authenticated)
                {
                    RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NOAUTH);
                    resp.serialize(*out);
                    continue;
                }

                String db_name = server.config().getString(Poco::format("redis.db._%d.database", select_req.getDb()), "");
                String table_name = server.config().getString(Poco::format("redis.db._%d.table", select_req.getDb()), "");
                String column_name = server.config().getString(Poco::format("redis.db._%d.column", select_req.getDb()), "");
                String col_separator = server.config().getString(Poco::format("redis.db._%d.column_separator", select_req.getDb()), "");
                if (db_name.empty() || table_name.empty() || column_name.empty())
                {
                    RedisProtocol::ErrorResponse resp(Poco::format("Database, table or column is not set for %d", select_req.getDb()));
                    resp.serialize(*out);
                    continue;
                }

                if (!table_ptr || db != select_req.getDb())
                {
                    try
                    {
                        auto db_ptr = DatabaseCatalog::instance().getDatabase(db_name, server.context());
                        table_ptr = db_ptr->getTable(table_name, server.context());
                    }
                    catch (const Exception & e)
                    {
                        RedisProtocol::ErrorResponse resp(e.message());
                        resp.serialize(*out);
                        continue;
                    }
                    table = dynamic_cast<IKeyValueStorage *>(table_ptr.get());
                    if (table == nullptr)
                    {
                        table_ptr.reset();
                        RedisProtocol::ErrorResponse resp(
                            Poco::format("Selected table %s in database %s doesnt support key-value operations", table_name, db_name));
                        resp.serialize(*out);
                        continue;
                    }
                    db = select_req.getDb();
                    column = column_name;
                    if (col_separator.empty())
                        column_separator = ',';
                    else
                        column_separator = col_separator[0];
                }

                RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
                resp.serialize(*out);
            }
            else if (req.getMethod() == "reset")
            {
                LOG_DEBUG(log, "RESET request");

                db = 0;
                table_ptr.reset();
                table = nullptr;

                RedisProtocol::SimpleStringResponse resp("RESET");
                resp.serialize(*out);
            }
            else if (req.getMethod() == "quit")
            {
                LOG_DEBUG(log, "QUIT request");
                RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
                resp.serialize(*out);
                return;
            }
            else
            {
                throw Exception(
                    Poco::format("%s (%s)", RedisProtocol::Message::UNKNOWNCOMMAND, req.getMethod()), ErrorCodes::NOT_IMPLEMENTED);
            }
        }
    }
    catch (const Poco::Exception & exc)
    {
        log->log(exc);
        RedisProtocol::ErrorResponse resp(exc.message());
        resp.serialize(*out);
    }
}

std::vector<std::optional<String>> RedisHandler::getValuesByKeysAndDefaultColumn(const std::vector<String> & keys)
{
    std::vector<std::optional<String>> result;
    result.resize(keys.size());

    Block sample_block = table->getInMemoryMetadataPtr()->getSampleBlock();

    PaddedPODArray<UInt8> null_map; // TODO: Logic with nullable (currenly thinks that defaults are normal values)
    Chunk chunk = getChunkByKeys(keys, &null_map);

    auto default_column_idx = sample_block.getPositionByName(column);
    auto default_column = chunk.getColumns()[default_column_idx];
    auto num_rows = default_column->size();

    if (num_rows != keys.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Some keys seem lost");

    for (size_t i = 0; i < num_rows; ++i)
        if (StringRef data = default_column->getDataAt(i); !data.empty())
            result[i] = default_column->getDataAt(i).toString();

    return result;
}

std::vector<std::optional<String>> RedisHandler::getValuesByKeyAndColumns(const String & key, const std::vector<String> & columns)
{
    std::vector<std::optional<String>> result;
    result.resize(columns.size());

    Block sample_block = table->getInMemoryMetadataPtr()->getSampleBlock();

    PaddedPODArray<UInt8> null_map; // TODO: Logic with nullable (currenly thinks that defaults are normal values)
    std::vector<String> keys = {key};
    Chunk chunk = getChunkByKeys(keys, &null_map);

    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto idx = sample_block.getPositionByName(columns[i]);
        result[i] = chunk.getColumns()[idx]->getDataAt(0).toString();
    }

    return result;
}

Chunk RedisHandler::getChunkByKeys(const std::vector<String> & keys, PaddedPODArray<UInt8> * null_map)
{
    auto primary_key = table->getPrimaryKey();
    auto row_size = getColumnsFromKey(keys[0]).size();
    if (primary_key.size() != row_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of columns in key: {} != {}", primary_key.size(), row_size);

    auto sample_block = table->getInMemoryMetadataPtr()->getSampleBlock();
    auto keys_columns = sample_block.cloneEmpty();
    for (const auto & col_name : keys_columns.getNames())
        if (std::find(primary_key.begin(), primary_key.end(), col_name) == primary_key.end())
            keys_columns.erase(col_name);

    auto keys_columns_mutable = keys_columns.mutateColumns();
    for (auto & col : keys_columns_mutable)
        col->reserve(keys.size());

    for (const auto & key : keys)
    {
        auto row = getColumnsFromKey(key);
        if (row.size() != row_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Each key must have the same number of columns!");
        for (size_t i = 0; i < row.size(); ++i)
            keys_columns_mutable[i]->insert(std::move(row[i]));
    }

    if (null_map != nullptr)
        null_map->resize(table->getColumnSizes().size());

    auto chunk = table->getByKeys(keys_columns.getColumnsWithTypeAndName(), sample_block, null_map, server.context());

    return chunk;
}

bool RedisHandler::validateColumns(const std::vector<String> & columns)
{
    Block sample_block = table->getInMemoryMetadataPtr()->getSampleBlock();
    auto col_names = sample_block.getNames();

    for (auto & col_name : columns)
        if (std::find(col_names.begin(), col_names.end(), col_name) == col_names.end())
            return false;

    return true;
}

std::vector<String> RedisHandler::getColumnsFromKey(const String & key) const
{
    std::vector<String> result;
    boost::split(result, key, [sep = column_separator](char c) { return c == sep; });
    return result;
}

bool RedisHandler::validateGetRequest()
{
    if (!authenticated)
    {
        RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NOAUTH);
        resp.serialize(*out);
        return false;
    }
    if (!table_ptr)
    {
        RedisProtocol::ErrorResponse resp(Poco::format("No table selected for %d", db));
        resp.serialize(*out);
        return false;
    }
    return true;
}

void RedisHandler::makeSecureConnection()
{
#if USE_SSL
    ss = std::make_shared<Poco::Net::SecureStreamSocket>(
        Poco::Net::SecureStreamSocket::attach(socket(), Poco::Net::SSLManager::instance().defaultServerContext()));
    in = std::make_shared<ReadBufferFromPocoSocket>(*ss);
    out = std::make_shared<WriteBufferFromPocoSocket>(*ss);
#endif
}
}
