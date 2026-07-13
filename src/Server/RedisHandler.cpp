#include <memory>

#include <Poco/Exception.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Session.h>
#include <Server/RedisHandler.h>
#include <Server/RedisProtocolMapping.h>
#include <Server/RedisProtocolRequest.h>
#include <Server/RedisProtocolResponse.h>
#include <Server/TCPServer.h>
#include <Storages/IStorage.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

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
    setThreadName(ThreadName::REDIS_HANDLER);
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::REDIS);
    SCOPE_EXIT({ session.reset(); });

    while (tcp_server.isOpen())
    {
        /// Blocks until the next request arrives (or the peer closes the connection).
        if (in->eof())
            break;

        try
        {
            if (!processRequest())
                break;
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
    LOG_DEBUG(log, "Redis connection closed");
}

bool RedisHandler::processRequest()
{
    SCOPE_EXIT(out->next());
    RedisProtocol::RedisRequest req;
    req.deserialize(*in);
    switch (req.getCommand())
    {
        /// Necessary for working with cli clients in interactive mode.
        case RedisProtocol::CommandType::COMMAND:
        {
            LOG_DEBUG(log, "COMMAND request");
            RedisProtocol::CommandRequest cmd_request(req);
            cmd_request.deserialize(*in);

            /// Just ignore it for now.

            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        /// Necessary for working with python clients.
        case RedisProtocol::CommandType::CLIENT:
        {
            LOG_DEBUG(log, "CLIENT request");
            RedisProtocol::CommandRequest client_request(req);
            client_request.deserialize(*in);

            /// Just ignore it for now.

            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::AUTH:
        {
            LOG_DEBUG(log, "AUTH request");
            RedisProtocol::CommandRequest auth_request(req);
            auth_request.deserialize(*in);

            /// TODO: add authentication.

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
            if (!redis_clickhouse_mapping.contains(selected_db))
            {
                if (!config->db_mapping.contains(selected_db))
                {
                    RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NO_SUCH_DB);
                    resp.serialize(*out);
                    return true;
                }
                initDB(selected_db);
            }

            db = selected_db;
            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::GET:
        {
            LOG_DEBUG(log, "GET request");
            RedisProtocol::GetRequest get_request(req);
            get_request.deserialize(*in);

            checkDBSet();

            auto redis_db = redis_clickhouse_mapping[db];
            if (redis_db->getType() != RedisProtocol::DBType::STRING)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "GET command can only be applied to a database of type string");

            auto table = redis_db->getTable();
            auto value_column = std::static_pointer_cast<RedisProtocol::RedisStringMapping>(redis_db)->getValueColumnName();
            auto result_chunk = table->getChunkByKeys({get_request.getKey()}, {value_column}, server.context());
            auto result = String(result_chunk.getColumns()[0]->getDataAt(0));

            RedisProtocol::BulkStringResponse resp(result);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::MGET:
        {
            LOG_DEBUG(log, "MGET request");
            RedisProtocol::MGetRequest mget_request(req);
            mget_request.deserialize(*in);

            checkDBSet();

            auto redis_db = redis_clickhouse_mapping[db];
            if (redis_db->getType() != RedisProtocol::DBType::STRING)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "MGET command can only be applied to a database of type string");

            auto table = redis_db->getTable();
            auto value_column = std::static_pointer_cast<RedisProtocol::RedisStringMapping>(redis_db)->getValueColumnName();

            std::vector<String> result;
            result.reserve(mget_request.getKeys().size());
            for (const auto & key : mget_request.getKeys())
            {
                auto result_chunk = table->getChunkByKeys({key}, {value_column}, server.context());
                result.push_back(String(result_chunk.getColumns()[0]->getDataAt(0)));
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

            checkDBSet();

            auto redis_db = redis_clickhouse_mapping[db];
            if (redis_db->getType() != RedisProtocol::DBType::HASH)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HGET command can only be applied to a database of type hash");

            auto table = redis_db->getTable();
            auto result_chunk = table->getChunkByKeys({hget_request.getKey()}, {hget_request.getField()}, server.context());
            auto result = String(result_chunk.getColumns()[0]->getDataAt(0));

            RedisProtocol::BulkStringResponse resp(result);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::HMGET:
        {
            LOG_DEBUG(log, "HMGET request");
            RedisProtocol::HMGetRequest hmget_request(req);
            hmget_request.deserialize(*in);

            checkDBSet();

            auto redis_db = redis_clickhouse_mapping[db];
            if (redis_db->getType() != RedisProtocol::DBType::HASH)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HMGET command can only be applied to a database of type hash");

            auto table = redis_db->getTable();
            auto result_chunk = table->getChunkByKeys({hmget_request.getKey()}, hmget_request.getFields(), server.context());

            std::vector<String> result;
            result.reserve(result_chunk.getNumColumns());
            for (const auto & column : result_chunk.getColumns())
                result.push_back(String(column->getDataAt(0)));

            RedisProtocol::ArrayResponse resp(result);
            resp.serialize(*out);
            return true;
        }
    }
}

void RedisHandler::initDB(UInt32 db_)
{
    const auto & mapping = config->db_mapping[db_];

    auto db_ptr = DatabaseCatalog::instance().getDatabase(mapping.clickhouse_db, server.context());
    if (db_ptr == nullptr)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Database {} does not exist", mapping.clickhouse_db);

    auto table_ptr = db_ptr->getTable(mapping.clickhouse_table, server.context());
    if (table_ptr == nullptr)
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Table {} does not exist in database {}", mapping.clickhouse_table, mapping.clickhouse_db);

    if (!table_ptr->supportsGetRequests())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Table {} configured for Redis database {} does not support get requests",
            mapping.clickhouse_table, db_);

    switch (mapping.db_type)
    {
        case RedisProtocol::DBType::STRING:
        {
            redis_clickhouse_mapping[db_]
                = std::make_shared<RedisProtocol::RedisStringMapping>(mapping.db_type, table_ptr, mapping.key_column, mapping.value_column);
            break;
        }
        case RedisProtocol::DBType::HASH:
        {
            redis_clickhouse_mapping[db_] = std::make_shared<RedisProtocol::RedisHashMapping>(mapping.db_type, table_ptr, mapping.key_column);
            break;
        }
    }
}

void RedisHandler::checkDBSet() const
{
    if (db == RedisProtocol::DB_MAX_NUM)
        throw Exception(ErrorCodes::INVALID_STATE, "Redis db not set");
}

}
