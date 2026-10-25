#include <memory>
#include <optional>

#include <Poco/Exception.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Session.h>
#include <Server/RedisHandler.h>
#include <Server/RedisProtocolMapping.h>
#include <Server/RedisProtocolRequest.h>
#include <Server/RedisProtocolResponse.h>
#include <Server/TCPServer.h>
#include <Storages/IStorage.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsSeconds receive_timeout;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int AUTHENTICATION_FAILED;
    extern const int INVALID_STATE;
    extern const int UNSUPPORTED_METHOD;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{

/// Builds the key column of a lookup directly from the arguments buffered in the request, so that the
/// payload of the keys is copied only once (and not into intermediate `Field`s).
template <typename GetKey>
ColumnsWithTypeAndName makeKeyColumns(const String & key_column_name, size_t num_keys, GetKey && get_key)
{
    auto column = ColumnString::create();
    column->reserve(num_keys);
    for (size_t i = 0; i < num_keys; ++i)
    {
        const String & key = get_key(i);
        column->insertData(key.data(), key.size());
    }
    return {{std::move(column), std::make_shared<DataTypeString>(), key_column_name}};
}

}

RedisHandler::RedisHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_)
    : Poco::Net::TCPServerConnection(socket_), server(server_), tcp_server(tcp_server_)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
}

void RedisHandler::run()
{
    setThreadName(ThreadName::REDIS_HANDLER);
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::REDIS);
    SCOPE_EXIT({ session.reset(); });

    /// The listener has an infinite receive timeout. Waiting for the next command is done by polling
    /// below, so this timeout only applies to a command that has been started but not sent completely.
    socket().setReceiveTimeout(server.context()->getSettingsRef()[Setting::receive_timeout]);

    while (tcp_server.isOpen())
    {
        /// Wait for the next request in short slices, so that an idle connection notices that the
        /// listener has been stopped (the accepted sockets are not closed on stop).
        bool has_data = false;
        while (!has_data && tcp_server.isOpen())
            has_data = in->poll(1000000);
        if (!has_data)
            break;

        /// The socket is readable, so this does not block: it only detects that the peer has closed the connection.
        if (in->eof())
            break;

        RedisProtocol::RedisRequest req;
        try
        {
            req.deserialize(*in);
        }
        catch (const Poco::Exception & exc)
        {
            /// The command could not be read to its end, so the position in the stream is unknown
            /// and the connection cannot be reused: report the error and close it.
            log->log(exc);
            RedisProtocol::ErrorResponse resp(exc.message());
            resp.serialize(*out);
            out->next();
            break;
        }

        try
        {
            if (!processRequest(req))
                break;
        }
        catch (const Poco::Exception & exc)
        {
            /// The whole command has been read, so the connection stays usable.
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
        db = RedisProtocol::DB_MAX_NUM;
        authenticated = false;
    }

    try
    {
        session->authenticate(user_name, password, socket().peerAddress());
        session->makeSessionContext();
        authenticated = true;
    }
    catch (...)
    {
        /// The session cannot be reused after a failed authentication attempt.
        session = std::make_unique<Session>(server.context(), ClientInfo::Interface::REDIS);
        throw;
    }
}

RedisHandler::CommandScope RedisHandler::prepareQueryContext(const String & command_name)
{
    if (!authenticated)
    {
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

    /// A fresh query context for every command, as for the other stateful protocols: creating it
    /// re-checks that the credentials of the user are still valid (`VALID UNTIL`), so an already
    /// authenticated connection stops working once they expire.
    query_context = session->makeQueryContext();
    query_context->setCurrentQueryId("");

    /// Attach the command to a query, as `executeQuery` does: otherwise the memory limits of the user
    /// (`max_memory_usage`, `max_memory_usage_for_user`) would not apply to the lookup results, and a
    /// small request repeating a key with a large value could exhaust the memory of the server.
    CommandScope scope;
    scope.query_scope = QueryScope::create(query_context);
    scope.process_list_entry = query_context->getProcessList().insert(
        "Redis " + command_name, /*normalized_query_hash=*/ 0, /*ast=*/ nullptr, query_context,
        clock_gettime_ns(CLOCK_MONOTONIC), /*is_internal=*/ false);
    query_context->setProcessListElement(scope.process_list_entry->getQueryStatus());
    return scope;
}

bool RedisHandler::processRequest(RedisProtocol::RedisRequest & req)
{
    SCOPE_EXIT(out->next());
    req.parse();
    switch (req.getCommand())
    {
        /// Necessary for working with cli clients in interactive mode.
        case RedisProtocol::CommandType::COMMAND:
        {
            LOG_DEBUG(log, "COMMAND request");
            /// Just ignore it (the arguments have already been consumed) for now.

            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        /// Necessary for working with python clients.
        case RedisProtocol::CommandType::CLIENT:
        {
            LOG_DEBUG(log, "CLIENT request");
            /// Just ignore it (the arguments have already been consumed) for now.

            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::AUTH:
        {
            LOG_DEBUG(log, "AUTH request");
            RedisProtocol::AuthRequest auth_request(req);
            auth_request.parse();

            authenticate(auth_request.getUser(), auth_request.getPassword());

            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::ECHO_COMMAND:
        {
            LOG_DEBUG(log, "ECHO request");
            RedisProtocol::EchoRequest echo_request(req);
            echo_request.parse();

            RedisProtocol::BulkStringResponse resp(echo_request.getCommandInput());
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::PING:
        {
            LOG_DEBUG(log, "PING request");
            RedisProtocol::PingRequest ping_request(req);
            ping_request.parse();

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
            select_request.parse();

            auto command_scope = prepareQueryContext("SELECT");

            auto selected_db = select_request.getDB();
            auto mapping = RedisProtocol::parseDBDescription(server.config(), selected_db);
            if (!mapping)
            {
                RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NO_SUCH_DB);
                resp.serialize(*out);
                return true;
            }

            /// Report a misconfigured mapping or an unusable table right away, not on the first lookup.
            auto resolved = resolveTable(selected_db, *mapping);

            /// For a string database, also check that the value column can represent a missing key
            /// as Nil: with no keys, `getBlockByKeys` only validates the return types, and throws
            /// for a value column whose type cannot be wrapped in `Nullable`.
            if (mapping->db_type == RedisProtocol::DBType::STRING)
                resolved.table->getBlockByKeys(
                    makeKeyColumns(mapping->key_column, 0, [](size_t) -> const String & { UNREACHABLE(); }),
                    {mapping->value_column},
                    query_context);

            db = selected_db;
            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::GET:
        {
            LOG_DEBUG(log, "GET request");
            RedisProtocol::GetRequest get_request(req);
            get_request.parse();

            auto command_scope = prepareQueryContext("GET");
            checkDBSet();

            auto mapping = getMapDescription(db);
            if (mapping.db_type != RedisProtocol::DBType::STRING)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "GET command can only be applied to a database of type string");

            auto [table, table_lock] = resolveTable(db, mapping);
            auto keys = makeKeyColumns(mapping.key_column, 1, [&](size_t) -> const String & { return get_request.getKey(); });
            auto result_block = table->getBlockByKeys(keys, {mapping.value_column}, query_context);

            RedisProtocol::BulkStringResponse resp(serializeValue(result_block.getByPosition(0), 0));
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::MGET:
        {
            LOG_DEBUG(log, "MGET request");
            RedisProtocol::MGetRequest mget_request(req);
            mget_request.parse();

            auto command_scope = prepareQueryContext("MGET");
            checkDBSet();

            auto mapping = getMapDescription(db);
            if (mapping.db_type != RedisProtocol::DBType::STRING)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "MGET command can only be applied to a database of type string");

            auto [table, table_lock] = resolveTable(db, mapping);

            /// All the keys are looked up in one call, so that a single command cannot mix values
            /// from different states of the table.
            const size_t num_keys = mget_request.getKeysCount();
            auto keys = makeKeyColumns(mapping.key_column, num_keys, [&](size_t i) -> const String & { return mget_request.getKey(i); });

            auto result_block = table->getBlockByKeys(keys, {mapping.value_column}, query_context);
            keys.clear();
            const auto & value_column = result_block.getByPosition(0);

            std::vector<std::optional<String>> result;
            result.reserve(num_keys);
            for (size_t row = 0; row < num_keys; ++row)
                result.push_back(serializeValue(value_column, row));

            RedisProtocol::ArrayResponse resp(result);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::HGET:
        {
            LOG_DEBUG(log, "HGET request");
            RedisProtocol::HGetRequest hget_request(req);
            hget_request.parse();

            auto command_scope = prepareQueryContext("HGET");
            checkDBSet();

            auto mapping = getMapDescription(db);
            if (mapping.db_type != RedisProtocol::DBType::HASH)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HGET command can only be applied to a database of type hash");

            auto [table, table_lock] = resolveTable(db, mapping);
            query_context->checkAccess(AccessType::SELECT, table->getStorageID(), Strings{mapping.key_column, hget_request.getField()});
            auto keys = makeKeyColumns(mapping.key_column, 1, [&](size_t) -> const String & { return hget_request.getKey(); });
            auto result_block = table->getBlockByKeys(keys, {hget_request.getField()}, query_context);

            RedisProtocol::BulkStringResponse resp(serializeValue(result_block.getByPosition(0), 0));
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::HMGET:
        {
            LOG_DEBUG(log, "HMGET request");
            RedisProtocol::HMGetRequest hmget_request(req);
            hmget_request.parse();

            auto command_scope = prepareQueryContext("HMGET");
            checkDBSet();

            auto mapping = getMapDescription(db);
            if (mapping.db_type != RedisProtocol::DBType::HASH)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HMGET command can only be applied to a database of type hash");

            auto [table, table_lock] = resolveTable(db, mapping);

            /// The access check takes views of the fields, so that only the column names passed to the
            /// lookup are a copy of the fields buffered in the request.
            std::vector<std::string_view> columns_to_check;
            columns_to_check.reserve(hmget_request.getFieldsCount() + 1);
            columns_to_check.push_back(mapping.key_column);
            for (size_t i = 0; i < hmget_request.getFieldsCount(); ++i)
                columns_to_check.push_back(hmget_request.getField(i));
            query_context->checkAccess(AccessType::SELECT, table->getStorageID(), columns_to_check);

            Names fields;
            fields.reserve(hmget_request.getFieldsCount());
            for (size_t i = 0; i < hmget_request.getFieldsCount(); ++i)
                fields.push_back(hmget_request.getField(i));

            auto keys = makeKeyColumns(mapping.key_column, 1, [&](size_t) -> const String & { return hmget_request.getKey(); });
            auto result_block = table->getBlockByKeys(keys, fields, query_context);

            std::vector<std::optional<String>> result;
            result.reserve(result_block.columns());
            for (size_t i = 0; i < result_block.columns(); ++i)
                result.push_back(serializeValue(result_block.getByPosition(i), 0));

            RedisProtocol::ArrayResponse resp(result);
            resp.serialize(*out);
            return true;
        }
    }
}

std::optional<String> RedisHandler::serializeValue(const ColumnWithTypeAndName & column, size_t row)
{
    if (column.column->isNullAt(row))
        return std::nullopt;

    WriteBufferFromOwnString wb;
    column.type->getDefaultSerialization()->serializeText(*column.column, row, wb, FormatSettings{});
    return wb.str();
}

RedisProtocol::MapDescription RedisHandler::getMapDescription(UInt32 db_) const
{
    /// Read the configuration again for every request: a session must not keep serving a mapping that
    /// has been changed or removed by `SYSTEM RELOAD CONFIG`.
    auto mapping = RedisProtocol::parseDBDescription(server.config(), db_);
    if (!mapping)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Redis database {} is not configured", db_);
    return *mapping;
}

RedisHandler::ResolvedTable RedisHandler::resolveTable(UInt32 db_, const RedisProtocol::MapDescription & mapping) const
{
    /// Check the privileges on the configured table before touching the catalog. Otherwise a user
    /// without `SELECT` on it could tell a missing table from a misconfigured one and from a working
    /// one by the error of the resolution and of the validation below. The commands additionally
    /// check the columns they read.
    Strings columns_to_check{mapping.key_column};
    if (mapping.db_type == RedisProtocol::DBType::STRING)
        columns_to_check.push_back(mapping.value_column);
    query_context->checkAccess(AccessType::SELECT, StorageID{mapping.clickhouse_db, mapping.clickhouse_table}, columns_to_check);

    /// Resolve the table again for every request: a session must not keep serving data from a table
    /// that has been dropped, renamed, or recreated in the meantime.
    auto table = DatabaseCatalog::instance().getTable(StorageID{mapping.clickhouse_db, mapping.clickhouse_table}, query_context);

    /// Hold a share lock for the duration of the command, as a regular read does, so that a concurrent
    /// `DROP TABLE` or `DETACH TABLE` cannot commit while the command is reading the table.
    auto lock = table->lockForShare(query_context->getInitialQueryId(), query_context->getSettingsRef()[Setting::lock_acquire_timeout]);

    validateTable(db_, mapping, table);
    return {std::move(table), std::move(lock)};
}

void RedisHandler::validateTable(UInt32 db_, const RedisProtocol::MapDescription & mapping, const StoragePtr & table_ptr) const
{
    /// A lookup goes straight to the storage, so the row policy filter that a regular `SELECT` would
    /// apply is not evaluated. Refuse to serve a table the current user has a row policy on, instead
    /// of exposing the rows that the policy hides.
    auto row_policy_filter = query_context->getRowPolicyFilter(
        mapping.clickhouse_db, mapping.clickhouse_table, RowPolicyFilterType::SELECT_FILTER);
    if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "Cannot read table {} configured for Redis database {}, because a row policy is applied on it",
            table_ptr->getStorageID().getNameForLogs(), db_);

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

    if (mapping.db_type == RedisProtocol::DBType::STRING && !sample_block.has(mapping.value_column))
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "There is no value_column {} configured for Redis database {} in table {}",
            mapping.value_column, db_, mapping.clickhouse_table);
}

void RedisHandler::checkDBSet() const
{
    if (db == RedisProtocol::DB_MAX_NUM)
        throw Exception(ErrorCodes::INVALID_STATE, "Redis db not set");
}

}
