#include <cstddef>
#include <memory>
#include <Poco/Format.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Columns/IColumn_fwd.h>

#include "Columns/IColumn.h"
#include "RedisHandler.h"
#include "Common/Exception.h"
#include "Common/logger_useful.h"
#include "Common/setThreadName.h"
#include "Core/Field.h"
#include "Interpreters/ClientInfo.h"
#include "Interpreters/Session.h"
#include "Server/RedisProtocolMapping.h"
#include "Server/RedisProtocolRequest.h"
#include "Server/RedisProtocolResponse.h"
#include "Storages/IStorage.h"
#include "Storages/PartitionCommands.h"
#include "Storages/RedisCommon.h"
#include "base/scope_guard.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_STATE;
    extern const int UNSUPPORTED_METHOD;
    extern const int INVALID_CONFIG_PARAMETER;
}

RedisHandler::RedisHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, RedisProtocol::ConfigPtr config_)
    : Poco::Net::TCPServerConnection(socket_), server(server_), tcp_server(tcp_server_), config(config_)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
}

void RedisHandler::run()
{
    setThreadName("RedisHandler");
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::REDIS);
    SCOPE_EXIT({ session.reset(); });

    while (tcp_server.isOpen())
    {
        while (in->eof());
        try {
            if (!processRequest()) { return; }
        }
        catch (const Poco::Exception & exc)
        {
            log->log(exc);
            RedisProtocol::ErrorResponse resp(exc.message());
            resp.serialize(*out);
            out->next();
        }
    }
    out->finalize();
    LOG_DEBUG(log, "redis connection closed");
}

bool RedisHandler::processRequest()
{
    SCOPE_EXIT(out->next());
    RedisProtocol::RedisRequest req;
    req.deserialize(*in);
    switch (req.getCommand())
    {
        // Necessary for working with cli-clients in interactive mode
        case RedisProtocol::CommandType::COMMAND:
        {
            LOG_DEBUG(log, "COMMAND request");
            RedisProtocol::CommandRequest cmd_request(req);
            cmd_request.deserialize(*in);

            // Just ignoring it for now.

            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        // Necessary for working with python-clients
        case RedisProtocol::CommandType::CLIENT:
        {
            LOG_DEBUG(log, "CLIENT request");
            RedisProtocol::CommandRequest client_request(req);
            client_request.deserialize(*in);

            // Just ignore it for now.
            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::AUTH:
        {
            LOG_DEBUG(log, "AUTH request");
            // TODO add authentication

            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::ECHO:
        {
            LOG_DEBUG(log, "ECHO request");
            RedisProtocol::EchoRequest echo_request(req);
            echo_request.deserialize(*in);

            RedisProtocol::BulkStringResponse resp(echo_request.getCommandInput());
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::PING:
        {
            LOG_DEBUG(log, "PING request");
            RedisProtocol::PingRequest ping_request(req);
            ping_request.deserialize(*in);

            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::PONG);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::QUIT:
        {
            LOG_DEBUG(log, "QUIT request");
            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return false;
        }
        case RedisProtocol::CommandType::SELECT:
        {
            LOG_DEBUG(log, "SELECT request");
            RedisProtocol::SelectRequest select_request(req);
            select_request.deserialize(*in);

            auto selected_db = select_request.getDB();
            if (redis_click_house_mapping.find(selected_db) != redis_click_house_mapping.end())
            {
                db = selected_db;
                RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
                resp.serialize(*out);
                return true;
            }

            if (config->db_mapping.find(selected_db) != config->db_mapping.end())
            {
                initDB(selected_db);
                db = selected_db;
                RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
                resp.serialize(*out);
                return true;
            }

            RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NO_SUCH_DB);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::GET:
        {
            LOG_DEBUG(log, "GET request");
            RedisProtocol::GetRequest get_request(req);
            get_request.deserialize(*in);

            isDBSet();

            auto redis_db = redis_click_house_mapping[db];
            auto db_type = redis_db->getType();
            if (db_type != RedisProtocol::DBType::STRING)
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "GET command can only be applied to STRING db"
                );
            }

            auto table = redis_db->getTable();
            if (!table->supportsGetRequests())
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Configured clickhouse table do not support get requests"
                );
            }

            auto value_column = std::dynamic_pointer_cast<RedisProtocol::RedisStringMapping>(table)->getValueColumnName();
            auto result_chunk = table->getChunkByKeys({get_request.getKey()}, {value_column}, server.context());
            auto result = result_chunk.getColumns()[0]->getDataAt(0).toString();

            RedisProtocol::BulkStringResponse resp(result);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::MGET:
        {
            LOG_DEBUG(log, "MGET request");
            RedisProtocol::MGetRequest mget_request(req);
            mget_request.deserialize(*in);

            isDBSet();

            auto redis_db = redis_click_house_mapping[db];
            auto db_type = redis_db->getType();
            if (db_type != RedisProtocol::DBType::STRING)
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "GET command can only be applied to STRING db"
                );
            }

            auto table = redis_db->getTable();
            if (!table->supportsGetRequests())
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Configured clickhouse table do not support get requests"
                );
            }

            size_t n = mget_request.getKeys().size();
            std::vector<String> result;
            for (size_t i = 0; i < n; ++i)
            {
                auto value_column = std::dynamic_pointer_cast<RedisProtocol::RedisStringMapping>(table)->getValueColumnName();
                auto result_chunk = table->getChunkByKeys({mget_request.getKeys()[i]}, {value_column}, server.context());
                result.push_back(result_chunk.getColumns()[0]->getDataAt(0).toString());
            }

            RedisProtocol::ArrayResponse resp(result);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::HGET:
        {
            LOG_DEBUG(log, "HGET request");
            RedisProtocol::HGetRequest hget_request(req);
            hget_request.deserialize(*in);

            isDBSet();

            auto redis_db = redis_click_house_mapping[db];
            auto db_type = redis_db->getType();
            if (db_type != RedisProtocol::DBType::HASH)
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "HGET command can only be applied to HASH db"
                );
            }

            auto table = redis_db->getTable();
            if (!table->supportsGetRequests())
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Configured clickhouse table do not support get requests"
                );
            }

            auto result_chunk = table->getChunkByKeys({hget_request.getKey()}, {hget_request.getField()}, server.context());

            auto result_column = result_chunk.getColumns()[0];

            auto result = result_column->getDataAt(0).toString();

            RedisProtocol::BulkStringResponse resp(result);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::HMGET:
        {
            LOG_DEBUG(log, "HMGET request");
            RedisProtocol::HMGetRequest hmget_request(req);
            hmget_request.deserialize(*in);

            isDBSet();

            auto redis_db = redis_click_house_mapping[db];
            auto db_type = redis_db->getType();
            if (db_type != RedisProtocol::DBType::HASH)
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "HMGET command can only be applied to HASH db"
                );
            }

            auto table = redis_db->getTable();
            if (!table->supportsGetRequests())
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Configured clickhouse table do not support get requests"
                );
            }

            auto result_chunk = table->getChunkByKeys({hmget_request.getKey()}, hmget_request.getFields(), server.context());
            auto result_column = result_chunk.getColumns()[0];
            std::vector<String> result;
            for (size_t i = 0; i < result_column->size(); ++i)
            {
                result.push_back(result_column->getDataAt(i).toString());
            }

            RedisProtocol::ArrayResponse resp(result);
            resp.serialize(*out);
            return true;
        }
    }
}

void RedisHandler::initDB(UInt32 db_)
{
    auto mapping = config->db_mapping[db_];

    auto db_ptr = DatabaseCatalog::instance().getDatabase(mapping.clickhouse_db, server.context());
    if (db_ptr == nullptr)
    {
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "clickhouse_db {} not exists",
            mapping.clickhouse_db
        );
    }

    auto table_ptr = db_ptr->getTable(mapping.clickhouse_table, server.context());
    if (table_ptr == nullptr)
    {
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "clickhouse_table {} not exists in db {}",
            mapping.clickhouse_table, mapping.clickhouse_db
        );
    }

    switch (mapping.db_type)
    {
        case RedisProtocol::DBType::STRING:
        {
            auto string_db = std::make_shared<RedisProtocol::RedisStringMapping>(mapping.db_type, table_ptr, mapping.key_column, mapping.value_column);
            redis_click_house_mapping[db_] = std::move(string_db);
            break;
        }
        case RedisProtocol::DBType::HASH:
        {
            auto hash_db = std::make_shared<RedisProtocol::RedisHashMapping>(mapping.db_type, table_ptr, mapping.key_column);
            redis_click_house_mapping[db_] = std::move(hash_db);
            break;
        }
    }
}

std::vector<std::string> RedisHandler::getValueByKey(const String & key)
{
    auto mapping = redis_click_house_mapping[db];
    auto table = mapping->getTable();

    table->getChunkByKeys({Field(key)}, {"surname"}, server.context());

    return {};
}

void RedisHandler::isDBSet() const
{
    if (db == RedisProtocol::DB_MAX_NUM)
        throw Exception(
        ErrorCodes::INVALID_STATE,
        "Redis db not set"
    );
}
}
