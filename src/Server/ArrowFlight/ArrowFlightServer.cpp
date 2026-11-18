#include <Server/ArrowFlight/ArrowFlightServer.h>

#if USE_ARROWFLIGHT

#include <Server/ArrowFlight/AuthMiddleware.h>
#include <Server/ArrowFlight/CallsData.h>
#include <Server/ArrowFlight/commandSelector.h>
#include <Server/ArrowFlight/PollSession.h>

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/quoteString.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/SettingsChanges.h>
#include <Common/SettingSource.h>
#include <Common/ThreadStatus.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCMessages.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/ArrowFlightSource.h>
#include <QueryPipeline/Pipe.h>
#include <Poco/FileStream.h>
#include <Poco/StreamCopier.h>
#include <Interpreters/ProcessList.h>

#include <arrow/flight/sql/FlightSql.pb.h>
#include <fmt/ranges.h>
#include <google/protobuf/descriptor.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int UNKNOWN_SETTING;
    extern const int SYNTAX_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}


using ArrowFlight::CallsData;
using ArrowFlight::Duration;
using ArrowFlight::hasTicketPrefix;
using ArrowFlight::hasPollDescriptorPrefix;
using ArrowFlight::PollDescriptorInfo;
using ArrowFlight::PollDescriptorWithExpirationTime;
using ArrowFlight::PollSession;
using ArrowFlight::Result;
using ArrowFlight::Status;
using ArrowFlight::Table;
using ArrowFlight::TablePtr;
using ArrowFlight::Timestamp;

namespace protocol = arrow::flight::protocol;


namespace
{
    String readFile(const String & filepath)
    {
        Poco::FileInputStream ifs(filepath);
        String buf;
        Poco::StreamCopier::copyToString(ifs, buf);
        return buf;
    }

    /// Splits a SQL query at '?' placeholder positions using the ClickHouse Lexer.
    /// Returns a vector of query parts: for "SELECT ? + ?" it returns ["SELECT ", " + ", ""].
    /// The number of parameters equals parts.size() - 1.
    std::vector<String> splitQueryAtPlaceholders(const String & query) // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        std::vector<String> parts; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        Lexer lexer(query.data(), query.data() + query.size());
        const char * prev_end = query.data();

        while (true)
        {
            Token token = lexer.nextToken();
            if (token.isEnd())
                break;

            if (token.type == TokenType::QuestionMark)
            {
                parts.emplace_back(prev_end, token.begin);
                prev_end = token.end;
            }
        }

        parts.emplace_back(prev_end, query.data() + query.size());
        return parts;
    }

    /// Builds a SQL query from pre-split parts by joining them with "NULL".
    /// Used for syntax validation and schema inference during CreatePreparedStatement.
    String buildQueryWithNULLs(const std::vector<String> & query_parts) // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        String result;
        for (size_t i = 0; i < query_parts.size(); ++i)
        {
            if (i > 0)
                result += "NULL";
            result += query_parts[i];
        }
        return result;
    }

    /// Converts binary data to a ClickHouse SQL expression using unhex().
    /// This ensures the column name in the result schema is valid UTF-8
    /// (e.g. "unhex('AABB')"), because ClickHouse uses the expression text as the column name
    /// and Arrow requires field names to be valid UTF-8.
    String binaryToSQLExpression(std::string_view value)
    {
        static constexpr char hex_digits[] = "0123456789ABCDEF";
        String result = "unhex('";
        result.reserve(7 + value.size() * 2 + 2);
        for (unsigned char c : value)
        {
            result.push_back(hex_digits[c >> 4]);
            result.push_back(hex_digits[c & 0x0F]);
        }
        result += "')";
        return result;
    }

    /// Converts the value of a bound parameter to a ClickHouse SQL literal, by the Arrow type of the parameter.
    String parameterToSQLLiteral(const ArrowIPC::ArrowField & field, const ColumnWithTypeAndName & column)
    {
        if (column.column->isNullAt(0))
            return "NULL";

        WriteBufferFromOwnString text;
        column.type->getDefaultSerialization()->serializeText(*column.column, 0, text, FormatSettings{});
        const String & value = text.str();

        using ArrowIPC::TypeKind;
        switch (field.type.kind)
        {
            case TypeKind::Null:
                return "NULL";
            case TypeKind::Bool:
                return column.column->getBool(0) ? "1" : "0";
            /// Numbers: their text is a valid numeric literal.
            case TypeKind::Int:
            case TypeKind::FloatingPoint:
            case TypeKind::Decimal:
                return value;
            case TypeKind::Utf8:
            case TypeKind::LargeUtf8:
            case TypeKind::Utf8View:
                return quoteString(value);
            /// Binary types: use unhex() so the column name in the result stays valid UTF-8.
            case TypeKind::Binary:
            case TypeKind::LargeBinary:
            case TypeKind::BinaryView:
            case TypeKind::FixedSizeBinary:
                return binaryToSQLExpression(column.column->getDataAt(0));
            /// Date/time types: the text is a human-readable string like "2021-01-01" that must be quoted - otherwise
            /// ClickHouse parses "2021-01-01" as 2021 - 1 - 1 = 2019. Other types (lists, structs, maps, ...) are
            /// quoted as a best-effort fallback.
            default:
                return quoteString(value);
        }
    }

    /// Builds a SQL query from pre-split parts by substituting bound parameter values.
    /// The query_parts were produced by splitQueryAtPlaceholders at CreatePreparedStatement time.
    Result<String> buildQueryWithValues(const std::vector<String> & query_parts, const std::optional<Strings> & params) // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        size_t num_params = query_parts.size() - 1;

        if (!params)
        {
            if (num_params > 0)
                return Status::Invalid("Parameters were not bound before executing a prepared statement");
            return buildQueryWithNULLs(query_parts);
        }

        if (params->size() != num_params)
            return Status::Invalid(
                "Prepared statement has ", num_params, " parameter(s) but ", params->size(), " value(s) were bound");

        String result;
        for (size_t i = 0; i < num_params; ++i)
        {
            result += query_parts[i];
            result += (*params)[i];
        }
        result += query_parts[num_params];
        return result;
    }

    /// Checks whether a SQL string is actually a prepared statement handle.
    bool isPreparedStatementHandle(const std::string & sql)
    {
        return sql.starts_with(ArrowFlight::PREPARED_STATEMENT_HANDLE_PREFIX);
    }

    /// Whether `value` has the form of an identifier generated by `CallsData`: the prefix and a UUID.
    bool isGeneratedIdentifier(const String & value, const String & prefix)
    {
        constexpr size_t uuid_text_size = 36;
        UUID uuid;
        return value.size() == prefix.size() + uuid_text_size && value.starts_with(prefix)
            && tryParseUUID({reinterpret_cast<const UInt8 *>(value.data() + prefix.size()), uuid_text_size}, uuid);
    }

    /// A command or a ticket comes from the client and can be the text of a query with secrets, so the log gets
    /// only an identifier generated by this server, the name of a known Arrow Flight SQL command, or the size.
    String commandForLogging(const String & cmd)
    {
        if (isGeneratedIdentifier(cmd, ArrowFlight::TICKET_PREFIX) || isGeneratedIdentifier(cmd, ArrowFlight::POLL_DESCRIPTOR_PREFIX))
            return cmd;
        google::protobuf::Any any_msg;
        if (cmd.size() <= static_cast<size_t>(std::numeric_limits<int>::max())
            && any_msg.ParseFromArray(cmd.data(), static_cast<int>(cmd.size()))
            && ArrowFlight::isArrowFlightSql(any_msg))
        {
            const auto & type_url = any_msg.type_url();
            const auto type_name = type_url.substr(type_url.find_last_of('/') + 1);
            if (const auto * message = google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(type_name))
                return String(message->full_name());
        }
        return fmt::format("({} bytes)", cmd.size());
    }

    String descriptorForLogging(const protocol::FlightDescriptor & descriptor)
    {
        switch (descriptor.type())
        {
            case protocol::FlightDescriptor::CMD:
                return fmt::format("<FlightDescriptor cmd={}>", commandForLogging(descriptor.cmd()));
            case protocol::FlightDescriptor::PATH:
                return fmt::format("<FlightDescriptor path='{}'>", fmt::join(descriptor.path(), "/"));
            default:
                return "<FlightDescriptor type='UNKNOWN'>";
        }
    }

    String flightInfoForLogging(const protocol::FlightInfo & info)
    {
        std::vector<String> tickets; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        tickets.reserve(info.endpoint_size());
        for (const auto & endpoint : info.endpoint())
            tickets.push_back(commandForLogging(endpoint.ticket().ticket()));
        return fmt::format(
            "<FlightInfo descriptor={} tickets=[{}] total_records={} total_bytes={}>",
            descriptorForLogging(info.flight_descriptor()), fmt::join(tickets, ", "), info.total_records(), info.total_bytes());
    }

    String pollInfoForLogging(const protocol::PollInfo & info)
    {
        return fmt::format(
            "<PollInfo info={} descriptor={}>",
            info.has_info() ? flightInfoForLogging(info.info()) : String("null"),
            info.has_flight_descriptor() ? descriptorForLogging(info.flight_descriptor()) : String("null"));
    }

    void setTimestamp(google::protobuf::Timestamp & out, Timestamp time)
    {
        const auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(time.time_since_epoch()).count();
        out.set_seconds(nanoseconds / 1'000'000'000);
        out.set_nanos(static_cast<Int32>(nanoseconds % 1'000'000'000));
    }

    void addEndpoint(protocol::FlightInfo & info, const String & ticket, const std::optional<Timestamp> & expiration_time)
    {
        auto & endpoint = *info.add_endpoint();
        endpoint.mutable_ticket()->set_ticket(ticket);
        if (expiration_time)
            setTimestamp(*endpoint.mutable_expiration_time(), *expiration_time);
    }

    /// As `arrow::flight::FlightInfo::Make`, without the endpoints.
    void fillFlightInfo(
        protocol::FlightInfo & info, const String & schema, const protocol::FlightDescriptor & descriptor, Int64 total_records,
        Int64 total_bytes, const String & app_metadata = {})
    {
        info.set_schema(schema);
        *info.mutable_flight_descriptor() = descriptor;
        info.set_total_records(total_records);
        info.set_total_bytes(total_bytes);
        info.set_ordered(true);
        info.set_app_metadata(app_metadata);
    }

    [[nodiscard]] Result<String> convertPathToSQL(const google::protobuf::RepeatedPtrField<std::string> & path, bool for_put_operation)
    {
        if (path.size() != 1)
            return Status::Invalid("Flight descriptor's path should be one-component (got ", path.size(), " components)");
        if (path[0].empty())
            return Status::Invalid("Flight descriptor's path should specify the name of a table");
        const String & table_name = path[0];
        if (for_put_operation)
            return "INSERT INTO " + backQuoteIfNeed(table_name) + " FORMAT Arrow";
        return "SELECT * FROM " + backQuoteIfNeed(table_name);
    }

    /// For method doGet() the pipeline should have an output.
    [[nodiscard]] Status checkPipelineIsPulling(const QueryPipeline & pipeline)
    {
        if (!pipeline.pulling())
            return Status::Invalid("Query doesn't allow pulling data, use method doPut() with this kind of query");
        return Status::OK();
    }

    /// We don't allow custom formats except "Arrow" because they can't work with ArrowFlight.
    [[nodiscard]] Status checkNoCustomFormat(ASTPtr ast)
    {
        if (const auto * ast_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
        {
            if (ast_with_output->format_ast && (getIdentifierName(ast_with_output->format_ast) != "Arrow"))
                return Status::ExecutionError("Invalid format, only 'Arrow' format is supported");
        }
        else if (const auto * insert = dynamic_cast<const ASTInsertQuery *>(ast.get()))
        {
            if (!insert->format.empty() && insert->format != "Values" && insert->format != "Arrow")
                return Status::ExecutionError("Invalid format (", insert->format, "), only 'Arrow' format is supported");
        }
        return Status::OK();
    }

    /// The message of Arrow IPC that a `FlightData` carries, or none for a `FlightData` with metadata only.
    std::optional<ArrowIPC::IPCMessage> toIPCMessage(protocol::FlightData & data)
    {
        if (data.data_header().empty())
            return std::nullopt;
        ArrowIPC::IPCMessage message;
        message.header = std::move(*data.mutable_data_header());
        message.body = std::move(*data.mutable_data_body());
        return message;
    }

    protocol::FlightData toFlightData(const ArrowIPC::IPCMessage & message)
    {
        protocol::FlightData data;
        data.set_data_header(message.header);
        data.set_data_body(message.body);
        return data;
    }

    /// The values of the parameters a client binds to a prepared statement with `DoPut`, as SQL literals: the
    /// messages of an Arrow IPC stream of at most one row. None when no row is bound.
    Result<std::optional<Strings>> readParameterLiterals(VectorWithMemoryTracking<ArrowIPC::IPCMessage> messages, const ContextPtr & context)
    {
        if (messages.empty())
            return std::optional<Strings>{};

        const auto schema = ArrowIPC::parseSchemaMessage(messages.front().header);
        auto format_settings = getFormatSettings(context);

        auto make_buffer = [&messages]
        {
            return std::make_unique<ArrowIPC::IPCMessagesReadBuffer>(
                [&messages, i = size_t(0)](ArrowIPC::IPCMessage & message) mutable
                {
                    if (i >= messages.size())
                        return false;
                    message = messages[i++];
                    return true;
                });
        };

        auto schema_buffer = make_buffer();
        ArrowIPCSchemaReader schema_reader(*schema_buffer, /* stream_ */ true, format_settings);
        Block header;
        for (const auto & [name, type] : schema_reader.readSchema())
            header.insert({type->createColumn(), type, name});
        if (header.columns() != schema.fields.size())
            return Status::NotImplemented("The types of some of the bound parameters are not supported");

        auto data_buffer = make_buffer();
        auto input = std::make_shared<ArrowIPCBlockInputFormat>(
            *data_buffer, std::make_shared<const Block>(header), /* stream_ */ true, DEFAULT_BLOCK_SIZE, format_settings);
        QueryPipeline pipeline(Pipe(std::move(input)));
        PullingPipelineExecutor executor(pipeline);
        std::optional<Strings> literals;
        Block block;
        while (executor.pull(block))
        {
            Chunk chunk(block.getColumns(), block.rows());
            /// Some clients may send empty batches before the actual data.
            if (chunk.getNumRows() == 0)
                continue;
            if (literals)
                return Status::NotImplemented("Multiple parameter sets are not supported");
            if (chunk.getNumRows() > 1)
                return Status::NotImplemented("Multiple parameter sets are not supported (got ", chunk.getNumRows(), " rows)");

            literals.emplace();
            const auto & columns = chunk.getColumns();
            for (size_t i = 0; i < columns.size(); ++i)
            {
                const auto & header_column = header.getByPosition(i);
                literals->push_back(parameterToSQLLiteral(schema.fields[i], {columns[i], header_column.type, header_column.name}));
            }
        }
        return literals;
    }
}


/// The gRPC service: every call is authenticated and handled by the server.
class ArrowFlightServer::Service final : public protocol::FlightService::Service
{
public:
    explicit Service(ArrowFlightServer & server_) : server(server_) {}

    grpc::Status Handshake(
        grpc::ServerContext * context, grpc::ServerReaderWriter<protocol::HandshakeResponse, protocol::HandshakeRequest> *) override
    {
        /// There is no handshake: the call only authenticates the client, and the token it gets is in the headers.
        return server.handleCall(*context, "Handshake", [](const AuthMiddleware &) { return Status::OK(); });
    }

    grpc::Status ListFlights(grpc::ServerContext * context, const protocol::Criteria *, grpc::ServerWriter<protocol::FlightInfo> *) override
    {
        return server.handleCall(*context, "ListFlights", [](const AuthMiddleware &) { return Status::NotImplemented("NYI"); });
    }

    grpc::Status GetFlightInfo(grpc::ServerContext * context, const protocol::FlightDescriptor * request, protocol::FlightInfo * response) override
    {
        return server.handleCall(*context, "GetFlightInfo", [&](const AuthMiddleware & auth) { return server.getFlightInfo(auth, *request, *response); });
    }

    grpc::Status PollFlightInfo(grpc::ServerContext * context, const protocol::FlightDescriptor * request, protocol::PollInfo * response) override
    {
        return server.handleCall(*context, "PollFlightInfo", [&](const AuthMiddleware & auth) { return server.pollFlightInfo(auth, *request, *response); });
    }

    grpc::Status GetSchema(grpc::ServerContext * context, const protocol::FlightDescriptor * request, protocol::SchemaResult * response) override
    {
        return server.handleCall(*context, "GetSchema", [&](const AuthMiddleware & auth) { return server.getSchema(auth, *request, *response); });
    }

    grpc::Status DoGet(grpc::ServerContext * context, const protocol::Ticket * request, grpc::ServerWriter<protocol::FlightData> * writer) override
    {
        return server.handleCall(*context, "DoGet", [&](const AuthMiddleware & auth) { return server.doGet(auth, *request, *writer); });
    }

    grpc::Status DoPut(grpc::ServerContext * context, grpc::ServerReaderWriter<protocol::PutResult, protocol::FlightData> * stream) override
    {
        return server.handleCall(*context, "DoPut", [&](const AuthMiddleware & auth) { return server.doPut(auth, *stream); });
    }

    grpc::Status DoExchange(grpc::ServerContext * context, grpc::ServerReaderWriter<protocol::FlightData, protocol::FlightData> *) override
    {
        return server.handleCall(*context, "DoExchange", [](const AuthMiddleware &) { return Status::NotImplemented("NYI"); });
    }

    grpc::Status DoAction(grpc::ServerContext * context, const protocol::Action * request, grpc::ServerWriter<protocol::Result> * writer) override
    {
        return server.handleCall(*context, "DoAction", [&](const AuthMiddleware & auth) { return server.doAction(auth, *request, *writer); });
    }

    grpc::Status ListActions(grpc::ServerContext * context, const protocol::Empty *, grpc::ServerWriter<protocol::ActionType> *) override
    {
        return server.handleCall(*context, "ListActions", [](const AuthMiddleware &) { return Status::NotImplemented("NYI"); });
    }

private:
    ArrowFlightServer & server;
};


Result<ArrowFlightServer::DecodeResult> ArrowFlightServer::decodeDescriptor(
    const FlightDescriptor & descriptor,
    bool for_put_operation,
    const std::string & username) const
{
    switch (descriptor.type())
    {
        case FlightDescriptor::PATH:
        {
            FLIGHT_ASSIGN_OR_RAISE(auto sql, convertPathToSQL(descriptor.path(), for_put_operation))
            return DecodeResult {sql, {}, {}, {}};
        }
        case FlightDescriptor::CMD:
        {
            if (!for_put_operation && hasPollDescriptorPrefix(descriptor.cmd()))
                return Status::Invalid("Method GetFlightInfo cannot be called with a flight descriptor returned by method PollFlightInfo");

            auto res = ArrowFlight::commandSelector(descriptor.cmd());
            if (const auto * result_table = res.getTable())
            {
                FLIGHT_RETURN_NOT_OK(*result_table);
                return DecodeResult {{}, {}, {}, result_table->ValueUnsafe()};
            }
            const auto * sql_set = res.getSQLSet();

            /// If the command resolved to a prepared statement handle, look up the actual query.
            if (isPreparedStatementHandle(sql_set->sql))
            {
                FLIGHT_ASSIGN_OR_RAISE(auto ps_info, calls_data->getPreparedStatement(sql_set->sql, username))
                FLIGHT_ASSIGN_OR_RAISE(auto resolved_query, buildQueryWithValues(ps_info.query_parts, ps_info.bound_parameters))
                return DecodeResult {std::move(resolved_query), ArrowFlight::SchemaModifier{}, ArrowFlight::BlockModifier{}, ArrowFlight::TablePtr{}};
            }

            return DecodeResult {sql_set->sql, sql_set->schema_modifier, sql_set->block_modifier, {}};
        }
        default:
            return Status::TypeError("Flight descriptor has unknown type ", FlightDescriptor::DescriptorType_Name(descriptor.type()));
    }
}


ArrowFlightServer::ArrowFlightServer(IServer & server_, const Poco::Net::SocketAddress & address_to_listen_)
    : server(server_)
    , log(getLogger("ArrowFlightServer"))
    , address_to_listen(address_to_listen_)
    , tickets_lifetime_seconds(server.config().getUInt("arrowflight.tickets_lifetime_seconds", 600))
    , cancel_ticket_after_do_get(server.config().getBool("arrowflight.cancel_ticket_after_do_get", false))
    , poll_descriptors_lifetime_seconds(server.config().getUInt("arrowflight.poll_descriptors_lifetime_seconds", 600))
    , cancel_poll_descriptor_after_poll_flight_info(server.config().getBool("arrowflight.cancel_flight_descriptor_after_poll_flight_info", false))
    , max_prepared_statements_per_user(server.config().getUInt("arrowflight.max_prepared_statements_per_user", 100))
    , prepared_statements_lifetime_seconds(server.config().getInt("arrowflight.prepared_statements_lifetime_seconds", -1))
    , calls_data(
          std::make_unique<CallsData>(
              tickets_lifetime_seconds ? std::make_optional(std::chrono::seconds{tickets_lifetime_seconds}) : std::optional<Duration>{},
              poll_descriptors_lifetime_seconds ? std::make_optional(std::chrono::seconds{poll_descriptors_lifetime_seconds})
                                                : std::optional<Duration>{},
              prepared_statements_lifetime_seconds > 0 ? std::make_optional(std::chrono::seconds{prepared_statements_lifetime_seconds})
                                                       : std::optional<Duration>{},
              prepared_statements_lifetime_seconds == -1,
              max_prepared_statements_per_user,
              log))
{
}

void ArrowFlightServer::start()
{
    chassert(!initialized && !stopped);

    std::shared_ptr<grpc::ServerCredentials> credentials;
    if (server.config().getBool("arrowflight.enable_ssl", false))
    {
        grpc::SslServerCredentialsOptions options;
        grpc::SslServerCredentialsOptions::PemKeyCertPair key_cert_pair;
        key_cert_pair.cert_chain = readFile(server.config().getString("arrowflight.ssl_cert_file"));
        key_cert_pair.private_key = readFile(server.config().getString("arrowflight.ssl_key_file"));
        options.pem_key_cert_pairs.emplace_back(std::move(key_cert_pair));
        credentials = grpc::SslServerCredentials(options);
    }
    else
    {
        credentials = grpc::InsecureServerCredentials();
    }

    auth_factory = std::make_unique<AuthMiddlewareFactory>(server, *calls_data);
    service = std::make_unique<Service>(*this);

    grpc::ServerBuilder builder;
    int selected_port = 0;
    builder.AddListeningPort(address_to_listen.toString(), credentials, &selected_port);
    builder.RegisterService(service.get());
    /// As Arrow Flight servers do: a message is not limited in size.
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);
    grpc_server = builder.BuildAndStart();
    if (!grpc_server || selected_port == 0)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Failed init Arrow Flight Server: cannot listen on {}", address_to_listen.toString());

    initialized = true;

    if (tickets_lifetime_seconds || poll_descriptors_lifetime_seconds || prepared_statements_lifetime_seconds != 0)
    {
        cleanup_thread.emplace([this]
        {
            try
            {
                DB::setThreadName(ThreadName::ARROW_FLIGHT_EXPR);
                while (!stopped)
                {
                    calls_data->waitNextExpirationTime();
                    calls_data->cancelExpired();
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to cleanup");
            }
        });
    }
}

ArrowFlightServer::~ArrowFlightServer() = default;

void ArrowFlightServer::stop()
{
    if (!initialized)
        return;

    if (!stopped.exchange(true))
    {
        try
        {
            grpc_server->Shutdown();
            grpc_server->Wait();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to shutdown Arrow Flight");
        }

        calls_data->stopWaitingNextExpirationTime();
        if (cleanup_thread)
        {
            cleanup_thread->join();
            cleanup_thread.reset();
        }
        grpc_server.reset();
        service.reset();
        auth_factory.reset();
        calls_data.reset();
    }
}

UInt16 ArrowFlightServer::portNumber() const
{
    return address_to_listen.port();
}

grpc::Status ArrowFlightServer::handleCall(
    grpc::ServerContext & context, std::string_view method_name, const std::function<Status(const AuthMiddleware &)> & func)
{
    std::unique_ptr<AuthMiddleware> auth;
    Status auth_status;
    try
    {
        auth_status = auth_factory->startCall(context, auth);
    }
    catch (...)
    {
        auth_status = Status::Invalid(getCurrentExceptionMessage(/* with_stacktrace = */ false));
    }
    if (!auth_status.ok())
        return auth_status.toGrpcStatus(&context);

    SCOPE_EXIT({
        try
        {
            auth->callCompleted();
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("{}: failed to complete the call", method_name));
        }
    });

    return tryRunAndLogIfError(method_name, [&] { return func(*auth); }).toGrpcStatus(&context);
}

using SchemaAndTables = std::tuple<String, std::vector<TablePtr>>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

static Result<SchemaAndTables> executeSQLtoTables_impl(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    bool single_table,
    ArrowFlight::SchemaModifier schema_modifier = nullptr,
    ArrowFlight::BlockModifier block_modifier = nullptr
)
{
    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
    QueryScope query_scope = QueryScope::create(query_context);

    String schema;
    std::vector<TablePtr> tables; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

    bool query_finished = false;
    bool handling_exception = false;
    SCOPE_EXIT({
        if (!query_finished && !handling_exception)
            block_io.onCancelOrConnectionLoss();
    });

    try
    {
        FLIGHT_RETURN_NOT_OK(checkNoCustomFormat(ast));
        FLIGHT_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

        {
            PullingPipelineExecutor executor{block_io.pipeline};
            FLIGHT_ASSIGN_OR_RAISE(schema, ArrowFlight::flightSchema(executor.getHeader(), query_context, schema_modifier))

            std::optional<Block> header;
            Chunks chunks;
            Block block;
            while (executor.pull(block))
            {
                if (!block.empty())
                {
                    removeSpecialColumnRepresentations(block);
                    if (block_modifier)
                        block_modifier(query_context, block);
                    if (!header)
                        header = block.cloneEmpty();
                    chunks.emplace_back(Chunk{block.getColumns(), block.rows()});
                    if (!single_table)
                    {
                        tables.push_back(ArrowFlight::flightTable(*header, chunks, query_context, schema));
                        chunks.clear();
                    }
                }
            }

            if (!header)
                tables.push_back(ArrowFlight::flightTable(executor.getHeader(), {}, query_context, schema));
            else if (single_table)
                tables.push_back(ArrowFlight::flightTable(*header, chunks, query_context, schema));
        }

        block_io.onFinish();
        query_finished = true;
    }
    catch (...)
    {
        handling_exception = true;
        block_io.onException();
        throw;
    }

    return SchemaAndTables{schema, tables};
}

static Result<SchemaAndTables> executeSQLtoTables(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    ArrowFlight::SchemaModifier schema_modifier = nullptr,
    ArrowFlight::BlockModifier block_modifier = nullptr
)
{
    return executeSQLtoTables_impl(session, sql, false, schema_modifier, block_modifier);
}

/// Runs a query and returns its result as blocks.
static Blocks executeSQLtoBlocks(const std::shared_ptr<Session> & session, const std::string & sql)
{
    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId("");
    QueryScope query_scope = QueryScope::create(query_context);
    auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
    Blocks blocks;
    try
    {
        PullingPipelineExecutor executor{block_io.pipeline};
        Block block;
        while (executor.pull(block))
            if (!block.empty())
                blocks.push_back(std::move(block));
        block_io.onFinish();
    }
    catch (...)
    {
        block_io.onException();
        throw;
    }
    return blocks;
}

Status ArrowFlightServer::getFlightInfo(const AuthMiddleware & auth, const FlightDescriptor & request, protocol::FlightInfo & info)
{
    LOG_INFO(log, "GetFlightInfo is called for descriptor {}", descriptorForLogging(request));

    const auto & session = auth.getSession();

    std::string sql;
    ArrowFlight::SchemaModifier schema_modifier;
    ArrowFlight::BlockModifier block_modifier;
    TablePtr table;
    String schema;

    FLIGHT_ASSIGN_OR_RAISE(std::tie(sql, schema_modifier, block_modifier, table), decodeDescriptor(request, false, auth.getUsername()))
    chassert(!sql.empty() || table);

    Int64 total_rows = 0;
    Int64 total_bytes = 0;
    protocol::FlightInfo result;

    if (table)
    {
        schema = ArrowIPC::frameMessage(table->schema);
        total_rows = table->num_rows;
        total_bytes = table->num_bytes;
        auto ticket_info = calls_data->createTicket(table);
        addEndpoint(result, ticket_info->ticket, ticket_info->expiration_time);
    }
    else
    {
        // We generate a table for every chunk of data, which then produces ticket for every table
        // so clients can parallelize data retrieval.
        // However, it's unclear if this is necessary since we later indicate that data is ordered
        // and all endpoints are local. This forces clients to request data through the same connection,
        // and even with gRPC, clients are forced to prioritize the order.
        // TODO: Consider single ticket optimization for ordered local data to reduce overhead (executeSQLtoTable)
        std::vector<TablePtr> tables; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        FLIGHT_ASSIGN_OR_RAISE(std::tie(schema, tables), executeSQLtoTables(session, sql, schema_modifier, block_modifier))

        for (auto & t : tables)
        {
            total_rows += t->num_rows;
            total_bytes += t->num_bytes;
            auto ticket_info = calls_data->createTicket(t);
            addEndpoint(result, ticket_info->ticket, ticket_info->expiration_time);
        }
    }

    fillFlightInfo(result, schema, request, total_rows, total_bytes);
    info = std::move(result);

    LOG_INFO(log, "GetFlightInfo returns flight info {}", flightInfoForLogging(info));
    return Status::OK();
}


Status ArrowFlightServer::getSchema(const AuthMiddleware & auth, const FlightDescriptor & request, protocol::SchemaResult & schema_result)
{
    LOG_INFO(log, "GetSchema is called for descriptor {}", descriptorForLogging(request));

    const auto & session = auth.getSession();

    String schema;

    if ((request.type() == FlightDescriptor::CMD) && hasPollDescriptorPrefix(request.cmd()))
    {
        const String & poll_descriptor = request.cmd();
        FLIGHT_RETURN_NOT_OK(calls_data->extendPollDescriptorExpirationTime(poll_descriptor));
        FLIGHT_ASSIGN_OR_RAISE(auto poll_info, calls_data->getPollDescriptorInfo(poll_descriptor))
        schema = poll_info->schema;
    }
    else
    {
        std::string sql;
        ArrowFlight::SchemaModifier schema_modifier;
        ArrowFlight::BlockModifier block_modifier;
        TablePtr table;

        FLIGHT_ASSIGN_OR_RAISE(std::tie(sql, schema_modifier, block_modifier, table), decodeDescriptor(request, false, auth.getUsername()))
        chassert(!sql.empty() || table);

        if (table)
            schema = ArrowIPC::frameMessage(table->schema);
        else
        {
            auto query_context = session->makeQueryContext();
            query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
            QueryScope query_scope = QueryScope::create(query_context);

            auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

            bool query_finished = false;
            bool handling_exception = false;
            SCOPE_EXIT({
                if (!query_finished && !handling_exception)
                    block_io.onCancelOrConnectionLoss();
            });

            try
            {
                FLIGHT_RETURN_NOT_OK(checkNoCustomFormat(ast));
                FLIGHT_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

                {
                    PullingPipelineExecutor executor{block_io.pipeline};
                    FLIGHT_ASSIGN_OR_RAISE(schema, ArrowFlight::flightSchema(executor.getHeader(), query_context, schema_modifier))
                }

                block_io.onFinish();
                query_finished = true;
            }
            catch (...)
            {
                handling_exception = true;
                block_io.onException();
                throw;
            }
        }
    }

    schema_result.set_schema(schema);

    /// Not the field names: with secrets display enabled they contain the secret arguments.
    LOG_INFO(log, "GetSchema returns a schema with {} fields", ArrowIPC::parseSchemaMessage(schema).fields.size());
    return Status::OK();
}


Status ArrowFlightServer::pollFlightInfo(const AuthMiddleware & auth, const FlightDescriptor & request, protocol::PollInfo & info)
{
    LOG_INFO(log, "PollFlightInfo is called for descriptor {}", descriptorForLogging(request));

    const auto & session = auth.getSession();

    std::shared_ptr<const PollDescriptorInfo> poll_info;
    String schema;
    std::optional<PollDescriptorWithExpirationTime> next_poll_descriptor;
    bool should_cancel_poll_descriptor = false;

    FlightDescriptor original_flight_descriptor;
    std::string query_id;

    if ((request.type() == FlightDescriptor::CMD) && hasPollDescriptorPrefix(request.cmd()))
    {
        const String & poll_descriptor = request.cmd();
        FLIGHT_RETURN_NOT_OK(evaluatePollDescriptor(poll_descriptor));
        FLIGHT_RETURN_NOT_OK(calls_data->extendPollDescriptorExpirationTime(poll_descriptor));
        FLIGHT_ASSIGN_OR_RAISE(poll_info, calls_data->getPollDescriptorInfo(poll_descriptor))
        original_flight_descriptor = poll_info->original_flight_descriptor;
        query_id = poll_info->query_id;
        schema = poll_info->schema;
        if (poll_info->next_poll_descriptor)
            next_poll_descriptor = calls_data->getPollDescriptorWithExpirationTime(*poll_info->next_poll_descriptor);
        should_cancel_poll_descriptor = cancel_poll_descriptor_after_poll_flight_info;
    }
    else
    {
        std::string sql;
        ArrowFlight::SchemaModifier schema_modifier;
        ArrowFlight::BlockModifier block_modifier;
        TablePtr table;

        FLIGHT_ASSIGN_OR_RAISE(std::tie(sql, schema_modifier, block_modifier, table), decodeDescriptor(request, false, auth.getUsername()))
        chassert(!sql.empty() || table);

        if (table)
        {
            auto ticket_info = calls_data->createTicket(table);
            protocol::PollInfo result;
            auto & flight_info = *result.mutable_info();
            addEndpoint(flight_info, ticket_info->ticket, ticket_info->expiration_time);
            fillFlightInfo(flight_info, ArrowIPC::frameMessage(table->schema), request,
                static_cast<Int64>(table->num_rows), static_cast<Int64>(table->num_bytes));
            info = std::move(result);

            LOG_INFO(log, "PollFlightInfo returns {}", pollInfoForLogging(info));
            return Status::OK();
        }

        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.

        auto thread_group = ThreadGroup::createForQuery(query_context);
        CurrentThread::attachToGroup(thread_group);
        SCOPE_EXIT({ CurrentThread::detachFromGroupIfNotDetached(); });

        auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

        bool block_io_owned_here = true;
        SCOPE_EXIT({
            if (block_io_owned_here)
                block_io.onCancelOrConnectionLoss();
        });

        FLIGHT_RETURN_NOT_OK(checkNoCustomFormat(ast));
        FLIGHT_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

        block_io_owned_here = false;
        auto poll_session = std::make_unique<PollSession>(query_context, thread_group, std::move(block_io), schema_modifier, block_modifier);

        schema = poll_session->getSchema();

        original_flight_descriptor = request;
        query_id = query_context->getCurrentQueryId();
        auto next_info = calls_data->createPollDescriptor(std::move(poll_session), original_flight_descriptor, query_id);
        next_poll_descriptor = *next_info;
    }

    protocol::PollInfo result;
    auto & flight_info = *result.mutable_info();
    Int64 total_rows = 0;
    Int64 total_bytes = 0;

    std::vector<std::pair<String, std::optional<Timestamp>>> endpoints; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    while (poll_info)
    {
        if (poll_info->ticket)
            endpoints.emplace_back(*poll_info->ticket, calls_data->getTicketExpirationTime(*poll_info->ticket));
        if (poll_info->rows)
            total_rows += *poll_info->rows;
        if (poll_info->bytes)
            total_bytes += *poll_info->bytes;
        poll_info = poll_info->previous_info;
    }
    std::reverse(endpoints.begin(), endpoints.end());
    for (const auto & [ticket, expiration_time] : endpoints)
        addEndpoint(flight_info, ticket, expiration_time);

    fillFlightInfo(flight_info, schema, original_flight_descriptor, total_rows, total_bytes, query_id);

    if (next_poll_descriptor)
    {
        auto & next = *result.mutable_flight_descriptor();
        next.set_type(FlightDescriptor::CMD);
        next.set_cmd(next_poll_descriptor->poll_descriptor);
        if (next_poll_descriptor->expiration_time)
            setTimestamp(*result.mutable_expiration_time(), *next_poll_descriptor->expiration_time);
    }

    info = std::move(result);

    if (should_cancel_poll_descriptor)
        calls_data->cancelPollDescriptor(request.cmd());

    LOG_INFO(log, "PollFlightInfo returns {}", pollInfoForLogging(info));
    return Status::OK();
}


/// evaluatePollDescriptors() pulls a block from the query pipeline.
/// This function blocks until it either gets a nonempty block from the query pipeline or finds out that there will be no blocks anymore.
///
/// NOTE: The current implementation doesn't allow to set a timeout to avoid blocking calls as it's suggested in the documentation
/// for PollFlightInfo (see https://arrow.apache.org/docs/format/Flight.html#downloading-data-by-running-a-heavy-query).
Status ArrowFlightServer::evaluatePollDescriptor(const String & poll_descriptor)
{
    FLIGHT_ASSIGN_OR_RAISE(auto poll_session, calls_data->startEvaluation(poll_descriptor))

    if (!poll_session)
    {
        /// Already evaluated.
        FLIGHT_ASSIGN_OR_RAISE(auto info, calls_data->getPollDescriptorInfo(poll_descriptor))
        if (!info->evaluated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Session is not attached to non-evaluated poll descriptor {}", poll_descriptor);
        return *info->status;
    }

    ThreadGroupSwitcher thread_group_switcher{poll_session->getThreadGroup(), ThreadName::ARROW_FLIGHT};

    std::optional<String> ticket;
    try
    {
        UInt64 rows = 0;
        UInt64 bytes = 0;
        Block block;
        while (poll_session->getNextBlock(block))
        {
            if (block.empty())
                continue;

            auto header = block.cloneEmpty();
            rows = block.rows();
            bytes = block.bytes();
            Chunks chunks;
            chunks.emplace_back(Chunk{std::move(block).getColumns(), rows});
            auto table = ArrowFlight::flightTable(header, chunks, poll_session->queryContext(), poll_session->getSchema());
            auto ticket_info = calls_data->createTicket(table);
            ticket = ticket_info->ticket;
            break;
        }

        if (!ticket)
            poll_session->onFinish();
        calls_data->endEvaluation(poll_descriptor, ticket, rows, bytes, !ticket);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Poll: Failed to get next block");
        auto error_status = Status::ExecutionError("Poll: Failed to get next block: ", getCurrentExceptionMessage(/* with_stacktrace = */ false));
        calls_data->endEvaluationWithError(poll_descriptor, error_status);
        poll_session->onException();
        return error_status;
    }

    auto info_res = calls_data->getPollDescriptorInfo(poll_descriptor);
    if (!info_res.ok())
    {
        if (ticket)
            poll_session->onCancelOrConnectionLoss();
        return info_res.status();
    }
    const auto & info = info_res.ValueOrDie();
    if (!ticket)
        calls_data->eraseFlightDescriptorMapByDescriptor(poll_descriptor);
    else
        calls_data->createPollDescriptor(std::move(poll_session), info);

    return Status::OK();
}


Status ArrowFlightServer::doGet(const AuthMiddleware & auth, const protocol::Ticket & request, grpc::ServerWriter<protocol::FlightData> & writer)
{
    LOG_INFO(log, "DoGet is called for ticket {}", commandForLogging(request.ticket()));
    TablePtr table;
    bool should_cancel_ticket = false;

    const auto & session = auth.getSession();

    if (hasTicketPrefix(request.ticket()))
    {
        FLIGHT_ASSIGN_OR_RAISE(auto ticket_info, calls_data->getTicketInfo(request.ticket()))
        table = ticket_info->table;
        should_cancel_ticket = cancel_ticket_after_do_get;
    }
    else
    {
        const String & sql = request.ticket();

        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
        QueryScope query_scope = QueryScope::create(query_context);

        auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

        bool query_finished = false;
        bool handling_exception = false;
        SCOPE_EXIT({
            if (!query_finished && !handling_exception)
                block_io.onCancelOrConnectionLoss();
        });

        try
        {
            FLIGHT_RETURN_NOT_OK(checkNoCustomFormat(ast));
            FLIGHT_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

            {
                PullingPipelineExecutor executor{block_io.pipeline};

                Chunks chunks;
                Block block;
                while (executor.pull(block))
                {
                    if (!block.empty())
                    {
                        /// PullingPipelineExecutor returns sparse and other special column representations, which the writer does not read.
                        removeSpecialColumnRepresentations(block);
                        chunks.emplace_back(Chunk(block.getColumns(), block.rows()));
                    }
                }

                const auto & header = executor.getHeader();
                FLIGHT_ASSIGN_OR_RAISE(auto schema, ArrowFlight::flightSchema(header, query_context))
                table = ArrowFlight::flightTable(header, chunks, query_context, schema);
            }

            block_io.onFinish();
            query_finished = true;
        }
        catch (...)
        {
            handling_exception = true;
            block_io.onException();
            throw;
        }
    }

    if (!writer.Write(toFlightData(table->schema)))
        return Status::IOError("DoGet: the client closed the stream");
    for (const auto & message : table->batches)
        if (!writer.Write(toFlightData(message)))
            return Status::IOError("DoGet: the client closed the stream");

    if (should_cancel_ticket)
        calls_data->cancelTicket(request.ticket());

    LOG_INFO(log, "DoGet succeeded");
    return Status::OK();
}


Status ArrowFlightServer::doPut(
    const AuthMiddleware & auth, grpc::ServerReaderWriter<protocol::PutResult, protocol::FlightData> & stream)
{
    /// The first message carries the descriptor, and the schema of the data.
    protocol::FlightData first;
    if (!stream.Read(&first))
        return Status::Invalid("DoPut: the client sent no data");
    const FlightDescriptor request = first.flight_descriptor();
    LOG_INFO(log, "DoPut is called for descriptor {}", descriptorForLogging(request));

    const auto & session = auth.getSession();

    std::optional<ArrowIPC::IPCMessage> pending_first = toIPCMessage(first);
    auto next_message = [&](ArrowIPC::IPCMessage & message) -> bool
    {
        if (pending_first)
        {
            message = std::move(*pending_first);
            pending_first.reset();
            return true;
        }
        protocol::FlightData data;
        while (stream.Read(&data))
        {
            if (auto ipc_message = toIPCMessage(data))
            {
                message = std::move(*ipc_message);
                return true;
            }
        }
        return false;
    };

    auto write_metadata = [&](const String & metadata) -> Status
    {
        protocol::PutResult result;
        result.set_app_metadata(metadata);
        if (!stream.Write(result))
            return Status::IOError("DoPut: the client closed the stream");
        return Status::OK();
    };

    /// DoPut with CommandPreparedStatementQuery is parameter binding only (no execution).
    if (request.type() == FlightDescriptor::CMD)
    {
        google::protobuf::Any any_msg;
        if (any_msg.ParseFromArray(request.cmd().data(), static_cast<int>(request.cmd().size()))
            && any_msg.Is<protocol::sql::CommandPreparedStatementQuery>())
        {
            protocol::sql::CommandPreparedStatementQuery command;
            if (!any_msg.UnpackTo(&command))
                return Status::SerializationError("Deserialization of sql::CommandPreparedStatementQuery failed.");

            const auto & handle = command.prepared_statement_handle();
            LOG_DEBUG(log, "DoPut: binding parameters for prepared statement {}", handle);

            VectorWithMemoryTracking<ArrowIPC::IPCMessage> messages;
            ArrowIPC::IPCMessage message;
            while (next_message(message))
                messages.push_back(std::move(message));

            auto query_context = session->makeQueryContext();
            FLIGHT_ASSIGN_OR_RAISE(auto params, readParameterLiterals(std::move(messages), query_context))
            FLIGHT_RETURN_NOT_OK(calls_data->bindParameters(handle, auth.getUsername(), std::move(params)));

            /// Return DoPutPreparedStatementResult with the same handle.
            protocol::sql::DoPutPreparedStatementResult result;
            result.set_prepared_statement_handle(handle);
            FLIGHT_RETURN_NOT_OK(write_metadata(result.SerializeAsString()));

            LOG_INFO(log, "DoPut: parameter binding succeeded for prepared statement {}", handle);
            return Status::OK();
        }
    }

    bool dont_write_flight_sql_metadata = !ArrowFlight::flightDescriptorIsArrowFlightSqlCommand(request);

    std::string sql;
    ArrowFlight::SchemaModifier schema_modifier;
    ArrowFlight::BlockModifier block_modifier;
    TablePtr table;

    FLIGHT_ASSIGN_OR_RAISE(std::tie(sql, schema_modifier, block_modifier, table), decodeDescriptor(request, true, auth.getUsername()))
    /// DoPut command should only produce sql query
    chassert(!sql.empty() && !schema_modifier && !block_modifier && !table);

    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
    QueryScope query_scope = QueryScope::create(query_context);

    auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

    bool query_finished = false;
    bool handling_exception = false;
    SCOPE_EXIT({
        if (!query_finished && !handling_exception)
            block_io.onCancelOrConnectionLoss();
    });

    try
    {
        FLIGHT_RETURN_NOT_OK(checkNoCustomFormat(ast));
        auto & pipeline = block_io.pipeline;

        if (pipeline.pushing())
        {
            Block header = pipeline.getHeader();
            auto input = std::make_shared<ArrowFlightSource>(next_message, header, query_context);
            pipeline.complete(Pipe(std::move(input)));
        }
        else if (pipeline.pulling())
        {
            Block header = pipeline.getHeader();
            auto output = std::make_shared<NullSink>(std::make_shared<Block>(header));
            pipeline.complete(std::move(output));
        }

        if (pipeline.completed())
        {
            CompletedPipelineExecutor executor(pipeline);
            executor.execute();
        }

        block_io.onFinish();
        query_finished = true;
    }
    catch (...)
    {
        handling_exception = true;
        block_io.onException();
        throw;
    }

    if (!dont_write_flight_sql_metadata)
    {
        protocol::sql::DoPutUpdateResult update_result;
        if (auto element = query_context->getProcessListElement())
            update_result.set_record_count(element->getInfo().written_rows);
        else
            update_result.set_record_count(0);

        FLIGHT_RETURN_NOT_OK(write_metadata(update_result.SerializeAsString()));
    }

    LOG_INFO(log, "DoPut succeeded");

    return Status::OK();
}


Status ArrowFlightServer::tryRunAndLogIfError(std::string_view method_name, const std::function<Status()> & func) const
{
    DB::setThreadName(ThreadName::ARROW_FLIGHT);
    ThreadStatus thread_status;
    try
    {
        auto status = func();
        if (!status.ok())
            LOG_ERROR(log, "{} failed: {}", method_name, status.ToString());
        return status;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("{} failed", method_name));
        return Status::ExecutionError(method_name, " failed: ", getCurrentExceptionMessage(/* with_stacktrace = */ false));
    }
}


Status ArrowFlightServer::doAction(const AuthMiddleware & auth, const protocol::Action & action, grpc::ServerWriter<protocol::Result> & writer)
{
    LOG_INFO(log, "DoAction is called for action {}", action.type());

    const auto & session = auth.getSession();

    std::vector<String> results; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    if (action.type() == "CancelFlightInfo")
    {
        protocol::CancelFlightInfoRequest request;
        if (!request.ParseFromString(action.body()))
            return Status::Invalid("Not a valid CancelFlightInfoRequest");
        LOG_DEBUG(log, "CancelFlightInfo request {}", flightInfoForLogging(request.info()));
        const auto & query_id = request.info().app_metadata();
        protocol::CancelFlightInfoResult result;
        result.set_status(protocol::CANCEL_STATUS_NOT_CANCELLABLE);

        if (!query_id.empty())
        {
            auto & process_list = server.context()->getProcessList();
            auto cancel_result = process_list.sendCancelToQuery(query_id, auth.getUsername());
            if (cancel_result == CancellationCode::CancelSent)
            {
                result.set_status(protocol::CANCEL_STATUS_CANCELLED);

                for (const auto & pd : calls_data->collectPollDescriptorsForQueryId(query_id))
                    calls_data->cancelPollDescriptor(pd);
            }
        }

        results.push_back(result.SerializeAsString());
    }
    else if (action.type() == "SetSessionOptions")
    {
        protocol::SetSessionOptionsRequest request;
        if (!request.ParseFromString(action.body()))
            return Status::Invalid("Not a valid SetSessionOptionsRequest");
        protocol::SetSessionOptionsResult result;

        auto query_context = session->makeQueryContext();
        auto session_context = query_context->getSessionContext();

        /// Converts an Arrow Flight SessionOptionValue to a string representation suitable for Context::setSetting().
        auto to_string_value = [](const protocol::SessionOptionValue & value) -> String
        {
            switch (value.option_value_case())
            {
                case protocol::SessionOptionValue::kStringValue: return value.string_value();
                case protocol::SessionOptionValue::kBoolValue: return value.bool_value() ? "true" : "false";
                case protocol::SessionOptionValue::kInt64Value: return std::to_string(value.int64_value());
                case protocol::SessionOptionValue::kDoubleValue: return fmt::format("{}", value.double_value());
                case protocol::SessionOptionValue::kStringListValue:
                {
                    String res = "[";
                    const auto & strings = value.string_list_value().values();
                    for (int i = 0; i < strings.size(); ++i)
                    {
                        if (i > 0)
                            res += ",";
                        res += quoteString(strings[i]);
                    }
                    res += "]";
                    return res;
                }
                case protocol::SessionOptionValue::OPTION_VALUE_NOT_SET:
                    break;
            }
            /// A value that is not set means "reset to default" and is handled separately instead of calling this function.
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A session option without a value is converted to a string");
        };

        auto apply_option = [&](const std::string & setting, const protocol::SessionOptionValue & value)
        {
            if (!isValidIdentifier(setting))
            {
                (*result.mutable_errors())[setting].set_value(protocol::SetSessionOptionsResult::INVALID_NAME);
                return;
            }

            try
            {
                if (value.option_value_case() == protocol::SessionOptionValue::OPTION_VALUE_NOT_SET)
                {
                    /// No value means "reset to default" (SET setting = DEFAULT).
                    session_context->checkSettingsConstraintsForSettingsReset({setting}, SettingSource::QUERY);
                    session_context->resetSettingsToDefaultValue({setting});
                }
                else
                {
                    auto string_value = to_string_value(value);
                    SettingChange change{setting, Field{string_value}};
                    session_context->checkSettingsConstraints(change, SettingSource::QUERY);
                    session_context->setSetting(setting, string_value);
                }
            }
            catch (DB::Exception & e)
            {
                auto error_value = [&]()
                {
                    if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED || e.code() == ErrorCodes::SYNTAX_ERROR)
                        return protocol::SetSessionOptionsResult::INVALID_VALUE;
                    else if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                        return protocol::SetSessionOptionsResult::INVALID_NAME;
                    else
                        return protocol::SetSessionOptionsResult::UNSPECIFIED;
                }();

                (*result.mutable_errors())[setting].set_value(error_value);
            }
        };

        /// The options arrive in a map with no order, so `profile` goes first for its constraints to bind the rest.
        const auto & options = request.session_options();
        if (auto profile = options.find("profile"); profile != options.end())
            apply_option(profile->first, profile->second);
        for (const auto & [setting, value] : options)
        {
            if (setting != "profile")
                apply_option(setting, value);
        }

        results.push_back(result.SerializeAsString());
    }
    else if (action.type() == "GetSessionOptions")
    {
        protocol::GetSessionOptionsRequest request;
        if (!request.ParseFromString(action.body()))
            return Status::Invalid("Not a valid GetSessionOptionsRequest");
        protocol::GetSessionOptionsResult result;

        for (const auto & block : executeSQLtoBlocks(session, "SELECT name, value FROM system.settings"))
        {
            const auto names = block.getByPosition(0).column->convertToFullColumnIfConst();
            const auto values = block.getByPosition(1).column->convertToFullColumnIfConst();
            for (size_t i = 0; i < block.rows(); ++i)
            {
                if (names->isNullAt(i) || values->isNullAt(i))
                    continue;
                (*result.mutable_session_options())[String(names->getDataAt(i))].set_string_value(String(values->getDataAt(i)));
            }
        }

        results.push_back(result.SerializeAsString());
    }
    else if (action.type() == "CreatePreparedStatement")
    {

        protocol::sql::ActionCreatePreparedStatementRequest request;
        if (!request.ParseFromString(action.body()))
            return Status::Invalid("Could not deserialize ActionCreatePreparedStatementRequest.");

        if (request.has_transaction_id())
            return Status::NotImplemented("CreatePreparedStatement: transaction_id is not supported");

        const auto & query = request.query();

        if (query.empty())
            return Status::Invalid("CreatePreparedStatement: query must not be empty");

        /// Split the query at '?' placeholders once; reused at execution time.
        auto query_parts = splitQueryAtPlaceholders(query);

        /// Build a NULL-substituted query for syntax validation and schema inference.
        auto substituted_query = buildQueryWithNULLs(query_parts);

        /// Validate syntax and infer result schema by executing the substituted query up to schema stage.
        ArrowFlight::PreparedStatementInfo info;
        info.query_parts = std::move(query_parts);
        info.username = auth.getUsername();

        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId("");
        QueryScope query_scope = QueryScope::create(query_context);

        /// Parse the substituted query to validate syntax and determine query type.
        /// We only call executeQuery for SELECT-like queries (to infer the result schema).
        /// For other queries (INSERT, SET, DDL, etc.), parsing is sufficient — executing
        /// them would cause side effects (e.g. inserting rows or changing settings).
        ParserQuery parser(substituted_query.data() + substituted_query.size());
        auto ast = parseQuery(
            parser, substituted_query,
            query_context->getSettingsRef()[Setting::max_query_size],
            query_context->getSettingsRef()[Setting::max_parser_depth],
            query_context->getSettingsRef()[Setting::max_parser_backtracks]);
        FLIGHT_RETURN_NOT_OK(checkNoCustomFormat(ast));

        LOG_DEBUG(log, "CreatePreparedStatement request: query={}", ast->formatForLogging());

        if (dynamic_cast<const ASTSelectWithUnionQuery *>(ast.get()))
        {
            /// Try to infer the result schema by executing the NULL-substituted query.
            /// This may fail for queries where NULL is not a valid substitute (e.g. table
            /// function arguments like numbers(?)). In that case, we still create the
            /// prepared statement but without dataset_schema — the client will discover
            /// the schema at execution time.
            try
            {
                auto [_, block_io] = executeQuery(substituted_query, query_context, QueryFlags{}, QueryProcessingStage::Complete);

                try
                {
                    if (block_io.pipeline.pulling())
                    {
                        PullingPipelineExecutor executor{block_io.pipeline};
                        auto schema_res = ArrowFlight::flightSchema(executor.getHeader(), query_context);
                        if (schema_res.ok())
                            info.dataset_schema = std::move(schema_res).ValueUnsafe();
                    }
                    block_io.onCancelOrConnectionLoss();
                }
                catch (...)
                {
                    block_io.onException();
                    throw;
                }
            }
            catch (...)
            {
                LOG_DEBUG(log, "CreatePreparedStatement: schema inference failed for query '{}', "
                    "the prepared statement will be created without dataset_schema: {}",
                    ast->formatForLogging(), getCurrentExceptionMessage(/* with_stacktrace = */ false));
            }
        }

        std::optional<ArrowFlight::Duration> session_timeout_for_ps;
        if (calls_data->usesSessionTimeoutForPsLifetime() && auth.getSessionTimeout().count() > 0)
            session_timeout_for_ps = std::chrono::duration_cast<ArrowFlight::Duration>(auth.getSessionTimeout());

        FLIGHT_ASSIGN_OR_RAISE(auto handle, calls_data->createPreparedStatement(std::move(info), auth.getSessionId(), session_timeout_for_ps))

        /// Build the protobuf result.
        protocol::sql::ActionCreatePreparedStatementResult result;
        result.set_prepared_statement_handle(handle);

        if (auto ps_res = calls_data->getPreparedStatement(handle, auth.getUsername()); ps_res.ok())
        {
            const auto & ps_info = ps_res.ValueUnsafe();
            if (ps_info.dataset_schema)
                result.set_dataset_schema(*ps_info.dataset_schema);

            /// Build parameter schema: one field per '?' placeholder.
            /// Type is null because ClickHouse accepts any type and parses the value as a SQL literal.
            ArrowIPC::ArrowSchema param_schema;
            size_t num_params = ps_info.numParams();
            for (size_t i = 0; i < num_params; ++i)
            {
                ArrowIPC::ArrowField field;
                field.name = "parameter_" + std::to_string(i + 1);
                field.nullable = true;
                field.type.kind = ArrowIPC::TypeKind::Null;
                param_schema.fields.push_back(std::move(field));
            }
            result.set_parameter_schema(ArrowIPC::serializeSchemaMessage(param_schema));
        }

        results.push_back(result.SerializeAsString());
    }
    else if (action.type() == "ClosePreparedStatement")
    {

        protocol::sql::ActionClosePreparedStatementRequest request;
        if (!request.ParseFromString(action.body()))
            return Status::Invalid("Could not deserialize ActionClosePreparedStatementRequest.");

        const auto & handle = request.prepared_statement_handle();
        if (handle.empty())
        {
            const auto & sid = auth.getSessionId();
            LOG_DEBUG(log, "ClosePreparedStatement request: closing all statements for user={}, session={}",
                auth.getUsername(), sid.empty() ? "(none)" : sid);
            calls_data->closeAllPreparedStatements(auth.getUsername(), sid);
        }
        else
        {
            LOG_DEBUG(log, "ClosePreparedStatement request: handle={}", handle);
            calls_data->closePreparedStatement(handle, auth.getUsername());
        }

        /// ClosePreparedStatement has no response body per the spec.
    }
    else
        return Status::NotImplemented(action.type(), " is not supported");

    for (const auto & body : results)
    {
        protocol::Result result;
        result.set_body(body);
        if (!writer.Write(result))
            return Status::IOError("DoAction: the client closed the stream");
    }
    return Status::OK();
}

}

#endif
