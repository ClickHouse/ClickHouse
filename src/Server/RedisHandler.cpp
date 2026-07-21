#include <memory>
#include <optional>

#include <Poco/Exception.h>

#include <Access/Common/AccessFlags.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
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
    extern const int AUTHENTICATION_FAILED;
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

void RedisHandler::authenticate(const String & user_name, const String & password)
{
    /// A session (and the table mapping resolved with its context) cannot be reused
    /// after another authentication attempt, so start from scratch.
    if (authenticated)
    {
        session = std::make_unique<Session>(server.context(), ClientInfo::Interface::REDIS);
        query_context.reset();
        redis_clickhouse_mapping.clear();
        db = RedisProtocol::DB_MAX_NUM;
        authenticated = false;
    }

    try
    {
        session->authenticate(user_name, password, socket().peerAddress());
        query_context = session->makeSessionContext();
        authenticated = true;
    }
    catch (...)
    {
        /// The session cannot be reused after a failed authentication attempt.
        session = std::make_unique<Session>(server.context(), ClientInfo::Interface::REDIS);
        throw;
    }
}

void RedisHandler::ensureAuthenticated()
{
    if (authenticated)
        return;

    try
    {
        authenticate("default", "");
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "NOAUTH Authentication required (the AUTH command expects a password of a ClickHouse user, "
            "or a user name and a password)");
    }
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
            RedisProtocol::AuthRequest auth_request(req);
            auth_request.deserialize(*in);

            authenticate(auth_request.getUser(), auth_request.getPassword());

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

            ensureAuthenticated();

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

            ensureAuthenticated();
            checkDBSet();

            auto redis_db = redis_clickhouse_mapping[db];
            if (redis_db->getType() != RedisProtocol::DBType::STRING)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "GET command can only be applied to a database of type string");

            auto table = redis_db->getTable();
            auto value_column = std::static_pointer_cast<RedisProtocol::RedisStringMapping>(redis_db)->getValueColumnName();
            query_context->checkAccess(AccessType::SELECT, table->getStorageID(), Strings{redis_db->getKeyColumnName(), value_column});
            auto result_block = table->getBlockByKeys({get_request.getKey()}, {value_column}, query_context);

            RedisProtocol::BulkStringResponse resp(serializeValue(result_block.getByPosition(0)));
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::MGET:
        {
            LOG_DEBUG(log, "MGET request");
            RedisProtocol::MGetRequest mget_request(req);
            mget_request.deserialize(*in);

            ensureAuthenticated();
            checkDBSet();

            auto redis_db = redis_clickhouse_mapping[db];
            if (redis_db->getType() != RedisProtocol::DBType::STRING)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "MGET command can only be applied to a database of type string");

            auto table = redis_db->getTable();
            auto value_column = std::static_pointer_cast<RedisProtocol::RedisStringMapping>(redis_db)->getValueColumnName();
            query_context->checkAccess(AccessType::SELECT, table->getStorageID(), Strings{redis_db->getKeyColumnName(), value_column});

            std::vector<std::optional<String>> result;
            result.reserve(mget_request.getKeys().size());
            for (const auto & key : mget_request.getKeys())
            {
                auto result_block = table->getBlockByKeys({key}, {value_column}, query_context);
                result.push_back(serializeValue(result_block.getByPosition(0)));
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

            ensureAuthenticated();
            checkDBSet();

            auto redis_db = redis_clickhouse_mapping[db];
            if (redis_db->getType() != RedisProtocol::DBType::HASH)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HGET command can only be applied to a database of type hash");

            auto table = redis_db->getTable();
            query_context->checkAccess(AccessType::SELECT, table->getStorageID(), Strings{redis_db->getKeyColumnName(), hget_request.getField()});
            auto result_block = table->getBlockByKeys({hget_request.getKey()}, {hget_request.getField()}, query_context);

            RedisProtocol::BulkStringResponse resp(serializeValue(result_block.getByPosition(0)));
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::HMGET:
        {
            LOG_DEBUG(log, "HMGET request");
            RedisProtocol::HMGetRequest hmget_request(req);
            hmget_request.deserialize(*in);

            ensureAuthenticated();
            checkDBSet();

            auto redis_db = redis_clickhouse_mapping[db];
            if (redis_db->getType() != RedisProtocol::DBType::HASH)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HMGET command can only be applied to a database of type hash");

            auto table = redis_db->getTable();
            Strings columns_to_check{redis_db->getKeyColumnName()};
            columns_to_check.insert(columns_to_check.end(), hmget_request.getFields().begin(), hmget_request.getFields().end());
            query_context->checkAccess(AccessType::SELECT, table->getStorageID(), columns_to_check);
            auto result_block = table->getBlockByKeys({hmget_request.getKey()}, hmget_request.getFields(), query_context);

            std::vector<std::optional<String>> result;
            result.reserve(result_block.columns());
            for (size_t i = 0; i < result_block.columns(); ++i)
                result.push_back(serializeValue(result_block.getByPosition(i)));

            RedisProtocol::ArrayResponse resp(result);
            resp.serialize(*out);
            return true;
        }
    }
}

std::optional<String> RedisHandler::serializeValue(const ColumnWithTypeAndName & column)
{
    if (column.column->isNullAt(0))
        return std::nullopt;

    WriteBufferFromOwnString wb;
    column.type->getDefaultSerialization()->serializeText(*column.column, 0, wb, FormatSettings{});
    return wb.str();
}

void RedisHandler::initDB(UInt32 db_)
{
    const auto & mapping = config->db_mapping[db_];

    auto db_ptr = DatabaseCatalog::instance().getDatabase(mapping.clickhouse_db, query_context);
    if (db_ptr == nullptr)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Database {} does not exist", mapping.clickhouse_db);

    auto table_ptr = db_ptr->getTable(mapping.clickhouse_table, query_context);
    if (table_ptr == nullptr)
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Table {} does not exist in database {}", mapping.clickhouse_table, mapping.clickhouse_db);

    if (!table_ptr->supportsGetRequests())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Table {} configured for Redis database {} does not support get requests",
            mapping.clickhouse_table, db_);

    auto table_key_columns = table_ptr->getKeyColumnNamesForGetRequests();
    if (table_key_columns.size() != 1)
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Table {} configured for Redis database {} has {} key columns, but only tables with a single key column are supported",
            mapping.clickhouse_table, db_, table_key_columns.size());

    if (table_key_columns[0] != mapping.key_column)
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "key_column {} configured for Redis database {} does not match the key column {} of table {}",
            mapping.key_column, db_, table_key_columns[0], mapping.clickhouse_table);

    auto metadata_snapshot = table_ptr->getInMemoryMetadataPtr(query_context, false);
    auto sample_block = metadata_snapshot->getSampleBlock();

    /// Redis keys are strings.
    auto key_type = removeLowCardinality(removeNullable(sample_block.getByName(mapping.key_column).type));
    if (!isStringOrFixedString(key_type))
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Key column {} of table {} configured for Redis database {} must be of type String or FixedString, but it is {}",
            mapping.key_column, mapping.clickhouse_table, db_, sample_block.getByName(mapping.key_column).type->getName());

    switch (mapping.db_type)
    {
        case RedisProtocol::DBType::STRING:
        {
            if (!sample_block.has(mapping.value_column))
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "There is no value_column {} configured for Redis database {} in table {}",
                    mapping.value_column, db_, mapping.clickhouse_table);

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
