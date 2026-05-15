#include <Server/ArrowFlight/ArrowFlightServer.h>

#if USE_ARROWFLIGHT

#include <Server/ArrowFlight/AuthMiddleware.h>
#include <Server/ArrowFlight/CallsData.h>
#include <Server/ArrowFlight/commandSelector.h>
#include <Server/ArrowFlight/PollSession.h>

#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/quoteString.h>
#include <Common/CurrentThread.h>
#include <Common/SettingsChanges.h>
#include <Common/SettingSource.h>
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
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/ArrowFlightSource.h>
#include <QueryPipeline/Pipe.h>
#include <Poco/FileStream.h>
#include <Poco/StreamCopier.h>
#include <Interpreters/ProcessList.h>

#include <arrow/array/builder_binary.h>
#include <arrow/flight/sql/protocol_internal.h>
#include <arrow/ipc/writer.h>
#include <arrow/scalar.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int UNKNOWN_SETTING;
    extern const int SYNTAX_ERROR;
}

namespace Setting
{
    extern const SettingsBool output_format_arrow_unsupported_types_as_binary;
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
using ArrowFlight::Timestamp;


namespace
{
    /// Helper for std::visit with multiple lambda overloads
    /// Usage:
    ///   std::variant<int, std::string> v = 42;
    ///   auto result = std::visit(overloaded {
    ///       [](int i) { return std::to_string(i); },
    ///       [](const std::string& s) { return s; },
    ///       [](const auto& other) { return "unknown"; }
    ///   }, v);
    template <class... Ts> struct overloaded : Ts... { using Ts::operator()...; }; // NOLINT
    template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

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
    std::vector<String> splitQueryAtPlaceholders(const String & query)
    {
        std::vector<String> parts;
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
    String buildQueryWithNULLs(const std::vector<String> & query_parts)
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
    String binaryToSQLExpression(const String & value)
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

    /// Converts an Arrow scalar value to a ClickHouse SQL literal string.
    String arrowScalarToSQLLiteral(const std::shared_ptr<arrow::Scalar> & scalar)
    {
        if (!scalar || !scalar->is_valid)
            return "NULL";

        auto buffer_value = [](const std::shared_ptr<arrow::Buffer> & buf) -> String
        {
            return buf ? buf->ToString() : String{};
        };

        switch (scalar->type->id())
        {
            case arrow::Type::NA:
                return "NULL";

            case arrow::Type::BOOL:
                return std::static_pointer_cast<arrow::BooleanScalar>(scalar)->value ? "1" : "0";

            /// Integer types: ToString() produces valid numeric literals.
            case arrow::Type::INT8:
            case arrow::Type::INT16:
            case arrow::Type::INT32:
            case arrow::Type::INT64:
            case arrow::Type::UINT8:
            case arrow::Type::UINT16:
            case arrow::Type::UINT32:
            case arrow::Type::UINT64:
            /// Floating-point types: ToString() produces valid numeric literals.
            case arrow::Type::HALF_FLOAT:
            case arrow::Type::FLOAT:
            case arrow::Type::DOUBLE:
            /// Decimal types: ToString() produces numeric literals like "123.45".
            case arrow::Type::DECIMAL128:
            case arrow::Type::DECIMAL256:
                return scalar->ToString();

            /// String types: extract raw content from the buffer and escape.
            case arrow::Type::STRING:
                return quoteString(buffer_value(std::static_pointer_cast<arrow::StringScalar>(scalar)->value));
            case arrow::Type::LARGE_STRING:
                return quoteString(buffer_value(std::static_pointer_cast<arrow::LargeStringScalar>(scalar)->value));

            /// Binary types: use unhex() so the column name in the result stays valid UTF-8.
            case arrow::Type::BINARY:
                return binaryToSQLExpression(buffer_value(std::static_pointer_cast<arrow::BinaryScalar>(scalar)->value));
            case arrow::Type::LARGE_BINARY:
                return binaryToSQLExpression(buffer_value(std::static_pointer_cast<arrow::LargeBinaryScalar>(scalar)->value));
            case arrow::Type::FIXED_SIZE_BINARY:
                return binaryToSQLExpression(buffer_value(std::static_pointer_cast<arrow::FixedSizeBinaryScalar>(scalar)->value));

            /// Date/time types: ToString() produces human-readable strings like "2021-01-01"
            /// that must be quoted — otherwise ClickHouse parses "2021-01-01" as 2021 - 1 - 1 = 2019.
            case arrow::Type::DATE32:
            case arrow::Type::DATE64:
            case arrow::Type::TIMESTAMP:
            case arrow::Type::TIME32:
            case arrow::Type::TIME64:
            case arrow::Type::DURATION:
            default:
                /// For any remaining types (LIST, STRUCT, MAP, DICTIONARY, etc.),
                /// quote the string representation as a best-effort fallback.
                return quoteString(scalar->ToString());
        }
    }

    /// Builds a SQL query from pre-split parts by substituting bound parameter values.
    /// The query_parts were produced by splitQueryAtPlaceholders at CreatePreparedStatement time.
    arrow::Result<String> buildQueryWithValues(const std::vector<String> & query_parts, const std::shared_ptr<arrow::RecordBatch> & params)
    {
        size_t num_params = query_parts.size() - 1;

        if (!params || params->num_rows() == 0)
        {
            if (num_params > 0)
                return arrow::Status::Invalid("Parameters were not bound before executing a prepared statement");
            return buildQueryWithNULLs(query_parts);
        }

        if (params->num_rows() > 1)
            return arrow::Status::NotImplemented("Multiple parameter sets are not supported (got ", params->num_rows(), " rows)");

        if (static_cast<size_t>(params->num_columns()) != num_params)
            return arrow::Status::Invalid(
                "Prepared statement has ", num_params, " parameter(s) but ", params->num_columns(), " value(s) were bound");

        String result;
        for (size_t i = 0; i < num_params; ++i)
        {
            result += query_parts[i];
            ARROW_ASSIGN_OR_RAISE(auto scalar, params->column(static_cast<int>(i))->GetScalar(0))
            result += arrowScalarToSQLLiteral(scalar);
        }
        result += query_parts[num_params];
        return result;
    }

    /// Checks whether a SQL string is actually a prepared statement handle.
    bool isPreparedStatementHandle(const std::string & sql)
    {
        return sql.starts_with(ArrowFlight::PREPARED_STATEMENT_HANDLE_PREFIX);
    }

    arrow::flight::Location addressToArrowLocation(const Poco::Net::SocketAddress & address_to_listen, bool use_tls)
    {
        auto ip_to_listen = address_to_listen.host();
        auto port_to_listen = address_to_listen.port();

        /// Function arrow::flight::Location::ForGrpc*() builds an URL so it requires IPv6 address to be enclosed in brackets
        String host_component = (ip_to_listen.family() == Poco::Net::AddressFamily::IPv6) ? ("[" + ip_to_listen.toString() + "]") : ip_to_listen.toString();

        arrow::Result<arrow::flight::Location> parse_location_status;
        if (use_tls)
            parse_location_status = arrow::flight::Location::ForGrpcTls(host_component, port_to_listen);
        else
            parse_location_status = arrow::flight::Location::ForGrpcTcp(host_component, port_to_listen);

        if (!parse_location_status.ok())
        {
            throw Exception(
                ErrorCodes::UNKNOWN_EXCEPTION,
                "Invalid address {} for Arrow Flight Server: {}",
                address_to_listen.toString(),
                parse_location_status.status().ToString());
        }

        return std::move(parse_location_status).ValueOrDie();
    }

    [[nodiscard]] arrow::Result<String> convertPathToSQL(const std::vector<std::string> & path, bool for_put_operation)
    {
        if (path.size() != 1)
            return arrow::Status::Invalid("Flight descriptor's path should be one-component (got ", path.size(), " components)");
        if (path[0].empty())
            return arrow::Status::Invalid("Flight descriptor's path should specify the name of a table");
        const String & table_name = path[0];
        if (for_put_operation)
            return "INSERT INTO " + backQuoteIfNeed(table_name) + " FORMAT Arrow";
        return "SELECT * FROM " + backQuoteIfNeed(table_name);
    }

    [[nodiscard]] arrow::Result<String> convertGetPathToSQL(const std::vector<std::string> & path)
    {
        return convertPathToSQL(path, /* for_put_operation = */ false);
    }

    [[nodiscard]] arrow::Result<String> convertPutPathToSQL(const std::vector<std::string> & path)
    {
        return convertPathToSQL(path, /* for_put_operation = */ true);
    }

    /// For method doGet() the pipeline should have an output.
    [[nodiscard]] arrow::Status checkPipelineIsPulling(const QueryPipeline & pipeline)
    {
        if (!pipeline.pulling())
            return arrow::Status::Invalid("Query doesn't allow pulling data, use method doPut() with this kind of query");
        return arrow::Status::OK();
    }

    /// We don't allow custom formats except "Arrow" because they can't work with ArrowFlight.
    [[nodiscard]] arrow::Status checkNoCustomFormat(ASTPtr ast)
    {
        if (const auto * ast_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
        {
            if (ast_with_output->format_ast && (getIdentifierName(ast_with_output->format_ast) != "Arrow"))
                return arrow::Status::ExecutionError("Invalid format, only 'Arrow' format is supported");
        }
        else if (const auto * insert = dynamic_cast<const ASTInsertQuery *>(ast.get()))
        {
            if (!insert->format.empty() && insert->format != "Values" && insert->format != "Arrow")
                return arrow::Status::ExecutionError("Invalid format (", insert->format, "), only 'Arrow' format is supported");
        }
        return arrow::Status::OK();
    }

    /// Creates a converter to convert ClickHouse blocks to the Arrow format.
    std::shared_ptr<CHColumnToArrowColumn> createCHToArrowConverter(const Block & header, ContextPtr query_context)
    {
        CHColumnToArrowColumn::Settings arrow_settings;
        arrow_settings.output_string_as_string = true;
        arrow_settings.output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary];
        auto ch_to_arrow_converter = std::make_shared<CHColumnToArrowColumn>(header, "Arrow", arrow_settings);
        ch_to_arrow_converter->initializeArrowSchema();
        return ch_to_arrow_converter;
    }
}


arrow::Result<ArrowFlightServer::DecodeResult> ArrowFlightServer::decodeDescriptor(
    const arrow::flight::FlightDescriptor & descriptor,
    bool for_put_operation,
    const std::string & username) const
{
    switch (descriptor.type)
    {
        case arrow::flight::FlightDescriptor::PATH:
        {
            auto sql_res = for_put_operation ? convertPutPathToSQL(descriptor.path) : convertGetPathToSQL(descriptor.path);
            ARROW_RETURN_NOT_OK(sql_res);
            return DecodeResult {sql_res.ValueUnsafe(), {}, {}, {}};
        }
        case arrow::flight::FlightDescriptor::CMD:
        {
            if (!for_put_operation && hasPollDescriptorPrefix(descriptor.cmd))
                return arrow::Status::Invalid("Method GetFlightInfo cannot be called with a flight descriptor returned by method PollFlightInfo");

            auto res = ArrowFlight::commandSelector(descriptor.cmd);
            if (const auto * result_table = res.getTable())
            {
                ARROW_RETURN_NOT_OK(*result_table);
                return DecodeResult {{}, {}, {}, result_table->ValueUnsafe()};
            }
            const auto * sql_set = res.getSQLSet();

            /// If the command resolved to a prepared statement handle, look up the actual query.
            if (isPreparedStatementHandle(sql_set->sql))
            {
                auto ps_info_res = calls_data->getPreparedStatement(sql_set->sql, username);
                ARROW_RETURN_NOT_OK(ps_info_res);
                const auto & ps_info = ps_info_res.ValueUnsafe();
                auto resolved_query_res = buildQueryWithValues(ps_info.query_parts, ps_info.bound_parameters);
                ARROW_RETURN_NOT_OK(resolved_query_res);
                return DecodeResult {std::move(resolved_query_res).ValueUnsafe(), {}, {}, {}};
            }

            return DecodeResult {sql_set->sql, sql_set->schema_modifier, sql_set->block_modifier, {}};
        }
        default:
            return arrow::Status::TypeError("Flight descriptor has unknown type ", magic_enum::enum_name(descriptor.type));
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

    bool use_tls = server.config().getBool("arrowflight.enable_ssl", false);

    auto location = addressToArrowLocation(address_to_listen, use_tls);

    arrow::flight::FlightServerOptions options(location);
    options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
    options.middleware.emplace_back(AUTHORIZATION_MIDDLEWARE_NAME, std::make_shared<AuthMiddlewareFactory>(server, *calls_data));

    if (use_tls)
    {
        auto cert_path = server.config().getString("arrowflight.ssl_cert_file");
        auto key_path = server.config().getString("arrowflight.ssl_key_file");

        auto cert = readFile(cert_path);
        auto key = readFile(key_path);

        options.tls_certificates.push_back(arrow::flight::CertKeyPair{cert, key});
    }

    auto init_status = Init(options);
    if (!init_status.ok())
    {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed init Arrow Flight Server: {}", init_status.ToString());
    }

    initialized = true;

    server_thread.emplace([this]
    {
        try
        {
            DB::setThreadName(ThreadName::ARROW_FLIGHT_SERVER);
            if (stopped)
                return;
            auto serve_status = Serve();
            if (!serve_status.ok())
                LOG_ERROR(log, "Failed to serve Arrow Flight: {}", serve_status.ToString());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to serve Arrow Flight");
        }
    });

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
            auto status = Shutdown();
            if (!status.ok())
                LOG_ERROR(log, "Failed to shutdown Arrow Flight: {}", status.ToString());
            status = Wait();
            if (!status.ok())
                LOG_ERROR(log, "Failed to wait for shutdown Arrow Flight: {}", status.ToString());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to shutdown Arrow Flight");
        }
        if (server_thread)
        {
            server_thread->join();
            server_thread.reset();
        }

        calls_data->stopWaitingNextExpirationTime();
        if (cleanup_thread)
        {
            cleanup_thread->join();
            cleanup_thread.reset();
        }
        calls_data.reset();
    }
}

UInt16 ArrowFlightServer::portNumber() const
{
    return address_to_listen.port();
}

static size_t calculateTableBytes(const std::shared_ptr<arrow::Table>& table)
{
    int64_t total_bytes = 0;
    for (const auto & chunked_array : table->columns())
        for (const auto & array : chunked_array->chunks())
            for (const auto& buffer : array->data()->buffers)
                if (buffer)
                    total_bytes += buffer->size();
    return total_bytes;
}

static ColumnsWithTypeAndName getHeader(const ColumnsWithTypeAndName & columns)
{
    ColumnsWithTypeAndName res;
    for (const auto & column : columns)
        res.emplace_back(column.cloneEmpty());
    return res;
}

static std::shared_ptr<arrow::Table> getEmptyArrowTable(std::shared_ptr<arrow::Schema> schema)
{
    size_t columns_num = schema->num_fields();
    std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
    empty_columns.reserve(columns_num);

    for (size_t i = 0; i < columns_num; ++i)
        empty_columns.push_back(std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{}, schema->field(static_cast<int>(i))->type()));

    return arrow::Table::Make(schema, empty_columns);
}

static arrow::Result<std::tuple<std::shared_ptr<arrow::Schema>, std::vector<std::shared_ptr<arrow::Table>>>> executeSQLtoTables_impl(
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

    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::Table>> tables;

    auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

    bool query_finished = false;
    bool handling_exception = false;
    SCOPE_EXIT({
        if (query_finished)
            block_io.onFinish();
        else if (!handling_exception)
            block_io.onCancelOrConnectionLoss();
    });

    try
    {
        ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
        ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

        PullingPipelineExecutor executor{block_io.pipeline};
        schema = CHColumnToArrowColumn::calculateArrowSchema(
            executor.getHeader().getColumnsWithTypeAndName(),
            "Arrow",
            nullptr,
            {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]});

        if (schema_modifier)
        {
            auto status = schema_modifier(schema);
            ARROW_RETURN_NOT_OK(status);
            schema = status.ValueUnsafe();
        }

        std::optional<ColumnsWithTypeAndName> header;
        std::vector<Chunk> chunks;
        Block block;
        while (executor.pull(block))
        {
            if (!block.empty())
            {
                if (block_modifier)
                    block_modifier(query_context, block);
                if (!header)
                    header = getHeader(block.getColumnsWithTypeAndName());
                chunks.emplace_back(Chunk{block.getColumns(), block.rows()});
                if (!single_table)
                {
                    tables.emplace_back(
                        CHColumnToArrowColumn::calculateArrowTable(
                            *header, "Arrow", chunks,
                            {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]},
                            header->size(), schema));
                    chunks.clear();
                }
            }
        }

        if (!header)
            tables.emplace_back(getEmptyArrowTable(schema));
        else if (single_table)
            tables.emplace_back(
        CHColumnToArrowColumn::calculateArrowTable(
            *header, "Arrow", chunks,
            {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]},
            header->size(), schema));

        query_finished = true;
    }
    catch (...)
    {
        handling_exception = true;
        block_io.onException();
        throw;
    }

    return std::tuple{schema, tables};
}

static arrow::Result<std::tuple<std::shared_ptr<arrow::Schema>, std::vector<std::shared_ptr<arrow::Table>>>> executeSQLtoTables(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    ArrowFlight::SchemaModifier schema_modifier = nullptr,
    ArrowFlight::BlockModifier block_modifier = nullptr
)
{
    return executeSQLtoTables_impl(session, sql, false, schema_modifier, block_modifier);
}

static arrow::Result<std::tuple<std::shared_ptr<arrow::Schema>, std::shared_ptr<arrow::Table>>> executeSQLtoTable(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    ArrowFlight::SchemaModifier schema_modifier = nullptr,
    ArrowFlight::BlockModifier block_modifier = nullptr
)
{
    auto res = executeSQLtoTables_impl(session, sql, true, schema_modifier, block_modifier);
    ARROW_RETURN_NOT_OK(res);
    return std::tuple{std::get<0>(res.ValueUnsafe()), std::get<1>(res.ValueUnsafe()).front()};
}

arrow::Status ArrowFlightServer::GetFlightInfo(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::FlightInfo> * info)
{
    auto impl = [&]
    {
        LOG_INFO(log, "GetFlightInfo is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        std::string sql;
        ArrowFlight::SchemaModifier schema_modifier;
        ArrowFlight::BlockModifier block_modifier;
        std::shared_ptr<arrow::Table> table;
        std::shared_ptr<arrow::Schema> schema;

        ARROW_ASSIGN_OR_RAISE(std::tie(sql, schema_modifier, block_modifier, table), decodeDescriptor(request, false, auth.getUsername()))
        chassert(!sql.empty() || table);

        std::vector<arrow::flight::FlightEndpoint> endpoints;
        int64_t total_rows = 0;
        int64_t total_bytes = 0;

        if (table)
        {
            schema = table->schema();
            total_rows = table->num_rows();
            total_bytes = calculateTableBytes(table);
            auto ticket_info = calls_data->createTicket(table);
            arrow::flight::FlightEndpoint endpoint;
            endpoint.ticket = arrow::flight::Ticket(ticket_info->ticket);
            endpoint.expiration_time = ticket_info->expiration_time;
            endpoints.emplace_back(endpoint);
        }
        else
        {
            // We generate a table for every chunk of data, which then produces ticket for every table
            // so clients can parallelize data retrieval.
            // However, it's unclear if this is necessary since we later indicate that data is ordered
            // and all endpoints are local. This forces clients to request data through the same connection,
            // and even with gRPC, clients are forced to prioritize the order.
            // TODO: Consider single ticket optimization for ordered local data to reduce overhead (executeSQLtoTable)
            std::vector<std::shared_ptr<arrow::Table>> tables;
            ARROW_ASSIGN_OR_RAISE(std::tie(schema, tables) , executeSQLtoTables(session, sql, schema_modifier, block_modifier))

            for (auto & t : tables)
            {
                total_rows += t->num_rows();
                total_bytes += calculateTableBytes(t);
                auto ticket_info = calls_data->createTicket(t);
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket(ticket_info->ticket);
                endpoint.expiration_time = ticket_info->expiration_time;
                endpoints.emplace_back(endpoint);
            }
        }

        auto flight_info_res = arrow::flight::FlightInfo::Make(
            *schema,
            request,
            endpoints,
            total_rows,
            total_bytes,
            /* ordered = */ true);

        ARROW_RETURN_NOT_OK(flight_info_res);
        *info = std::make_unique<arrow::flight::FlightInfo>(std::move(flight_info_res).ValueUnsafe());

        LOG_INFO(log, "GetFlightInfo returns flight info {}", (*info)->ToString());
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("GetFlightInfo", impl);
}


arrow::Status ArrowFlightServer::GetSchema(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::SchemaResult> * schema_result)
{
    auto impl = [&]
    {
        LOG_INFO(log, "GetSchema is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        std::shared_ptr<arrow::Schema> schema;

        if ((request.type == arrow::flight::FlightDescriptor::CMD) && hasPollDescriptorPrefix(request.cmd))
        {
            const String & poll_descriptor = request.cmd;
            ARROW_RETURN_NOT_OK(calls_data->extendPollDescriptorExpirationTime(poll_descriptor));
            auto poll_info_res = calls_data->getPollDescriptorInfo(poll_descriptor);
            ARROW_RETURN_NOT_OK(poll_info_res);
            const auto & poll_info = poll_info_res.ValueOrDie();
            schema = poll_info->schema;
        }
        else
        {
            std::string sql;
            ArrowFlight::SchemaModifier schema_modifier;
            ArrowFlight::BlockModifier block_modifier;
            std::shared_ptr<arrow::Table> table;

            ARROW_ASSIGN_OR_RAISE(std::tie(sql, schema_modifier, block_modifier, table), decodeDescriptor(request, false, auth.getUsername()))
            chassert(!sql.empty() || table);

            if (table)
                schema = table->schema();
            else
            {
                auto query_context = session->makeQueryContext();
                query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
                QueryScope query_scope = QueryScope::create(query_context);

                auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

                bool query_finished = false;
                bool handling_exception = false;
                SCOPE_EXIT({
                    if (query_finished)
                        block_io.onFinish();
                    else if (!handling_exception)
                        block_io.onCancelOrConnectionLoss();
                });

                try
                {
                    ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
                    ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

                    PullingPipelineExecutor executor{block_io.pipeline};

                    schema = CHColumnToArrowColumn::calculateArrowSchema(
                        executor.getHeader().getColumnsWithTypeAndName(), "Arrow", nullptr,
                        {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]});
                    if (schema_modifier)
                    {
                        auto status = schema_modifier(schema);
                        ARROW_RETURN_NOT_OK(status);
                        schema = status.ValueUnsafe();
                    }

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

        auto schema_res = arrow::flight::SchemaResult::Make(*schema);
        ARROW_RETURN_NOT_OK(schema_res);
        *schema_result = std::make_unique<arrow::flight::SchemaResult>(*std::move(schema_res).ValueUnsafe());

        LOG_INFO(log, "GetSchema returns schema {}", schema->ToString());
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("GetSchema", impl);
}


arrow::Status ArrowFlightServer::PollFlightInfo(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::PollInfo> * info)
{
    auto impl = [&]
    {
        LOG_INFO(log, "PollFlightInfo is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        std::shared_ptr<const PollDescriptorInfo> poll_info;
        std::shared_ptr<arrow::Schema> schema;
        std::optional<PollDescriptorWithExpirationTime> next_poll_descriptor;
        bool should_cancel_poll_descriptor = false;

        arrow::flight::FlightDescriptor original_flight_descriptor;
        std::string query_id;

        if ((request.type == arrow::flight::FlightDescriptor::CMD) && hasPollDescriptorPrefix(request.cmd))
        {
            const String & poll_descriptor = request.cmd;
            ARROW_RETURN_NOT_OK(evaluatePollDescriptor(poll_descriptor));
            ARROW_RETURN_NOT_OK(calls_data->extendPollDescriptorExpirationTime(poll_descriptor));
            auto poll_info_res = calls_data->getPollDescriptorInfo(poll_descriptor);
            ARROW_RETURN_NOT_OK(poll_info_res);
            poll_info = poll_info_res.ValueOrDie();
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
            std::shared_ptr<arrow::Table> table;

            ARROW_ASSIGN_OR_RAISE(std::tie(sql, schema_modifier, block_modifier, table), decodeDescriptor(request, false, auth.getUsername()))
            chassert(!sql.empty() || table);

            if (table)
            {
                auto ticket_info = calls_data->createTicket(table);
                std::vector<arrow::flight::FlightEndpoint> endpoints;
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket(ticket_info->ticket);
                endpoint.expiration_time = ticket_info->expiration_time;
                endpoints.emplace_back(endpoint);

                auto flight_info_res = arrow::flight::FlightInfo::Make(*table->schema(), request, endpoints, table->num_rows(), calculateTableBytes(table), /* ordered = */ true);
                ARROW_RETURN_NOT_OK(flight_info_res);
                auto flight_info = std::make_unique<arrow::flight::FlightInfo>(flight_info_res.ValueOrDie());
                *info = std::make_unique<arrow::flight::PollInfo>(std::move(flight_info), std::nullopt, std::nullopt, std::nullopt);

                LOG_INFO(log, "PollFlightInfo returns {}", (*info)->ToString());
                return arrow::Status::OK();
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

            ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
            ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

            block_io_owned_here = false;
            auto poll_session = std::make_unique<PollSession>(query_context, thread_group, std::move(block_io), schema_modifier, block_modifier);

            schema = poll_session->getSchema();

            original_flight_descriptor = request;
            query_id = query_context->getCurrentQueryId();
            auto next_info = calls_data->createPollDescriptor(std::move(poll_session), original_flight_descriptor, query_id);
            next_poll_descriptor = *next_info;
        }

        std::vector<arrow::flight::FlightEndpoint> endpoints;
        int64_t total_rows = 0;
        int64_t total_bytes = 0;

        while (poll_info)
        {
            if (poll_info->ticket)
            {
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket{*poll_info->ticket};
                endpoint.expiration_time = calls_data->getTicketExpirationTime(*poll_info->ticket);
                endpoints.emplace_back(endpoint);
            }
            if (poll_info->rows)
                total_rows += *poll_info->rows;
            if (poll_info->bytes)
                total_bytes += *poll_info->bytes;
            poll_info = poll_info->previous_info;
        }
        std::reverse(endpoints.begin(), endpoints.end());

        auto flight_info_res = arrow::flight::FlightInfo::Make(*schema, original_flight_descriptor, endpoints, total_rows, total_bytes, /* ordered = */ true, query_id);
        ARROW_RETURN_NOT_OK(flight_info_res);
        std::unique_ptr<arrow::flight::FlightInfo> flight_info = std::make_unique<arrow::flight::FlightInfo>(flight_info_res.ValueOrDie());

        std::optional<arrow::flight::FlightDescriptor> next;
        std::optional<Timestamp> expiration_time;
        if (next_poll_descriptor)
        {
            next = arrow::flight::FlightDescriptor::Command(next_poll_descriptor->poll_descriptor);
            expiration_time = next_poll_descriptor->expiration_time;
        }

        *info = std::make_unique<arrow::flight::PollInfo>(std::move(flight_info), std::move(next), std::nullopt, expiration_time);

        if (should_cancel_poll_descriptor)
            calls_data->cancelPollDescriptor(request.cmd);

        LOG_INFO(log, "PollFlightInfo returns {}", (*info)->ToString());
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("PollFlightInfo", impl);
}


/// evaluatePollDescriptors() pulls a block from the query pipeline.
/// This function blocks until it either gets a nonempty block from the query pipeline or finds out that there will be no blocks anymore.
///
/// NOTE: The current implementation doesn't allow to set a timeout to avoid blocking calls as it's suggested in the documentation
/// for PollFlightInfo (see https://arrow.apache.org/docs/format/Flight.html#downloading-data-by-running-a-heavy-query).
arrow::Status ArrowFlightServer::evaluatePollDescriptor(const String & poll_descriptor)
{
    auto poll_session_res = calls_data->startEvaluation(poll_descriptor);
    ARROW_RETURN_NOT_OK(poll_session_res);
    auto poll_session = std::move(poll_session_res).ValueOrDie();

    if (!poll_session)
    {
        /// Already evaluated.
        auto info_res = calls_data->getPollDescriptorInfo(poll_descriptor);
        ARROW_RETURN_NOT_OK(info_res);
        const auto & info = info_res.ValueOrDie();
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

            auto header = getHeader(block.getColumnsWithTypeAndName());
            rows = block.rows();
            bytes = block.bytes();
            std::vector<Chunk> chunks;
            chunks.emplace_back(Chunk{std::move(block).getColumns(), rows});
            std::shared_ptr<arrow::Table> table = CHColumnToArrowColumn::calculateArrowTable(
                header, "Arrow", chunks,
                {.output_string_as_string = true, .output_unsupported_types_as_binary = poll_session->queryContext()->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]},
                header.size(), poll_session->getSchema());
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
        auto error_status = arrow::Status::ExecutionError("Poll: Failed to get next block: ", getCurrentExceptionMessage(/* with_stacktrace = */ false));
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

    return arrow::Status::OK();
}


arrow::Status ArrowFlightServer::DoGet(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::Ticket & request,
    std::unique_ptr<arrow::flight::FlightDataStream> * stream)
{
    auto impl = [&]
    {
        LOG_INFO(log, "DoGet is called for ticket {}", request.ticket);
        std::vector<Chunk> chunks;
        std::shared_ptr<arrow::Table> table;
        bool should_cancel_ticket = false;

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        if (hasTicketPrefix(request.ticket))
        {
            auto ticket_info_res = calls_data->getTicketInfo(request.ticket);
            ARROW_RETURN_NOT_OK(ticket_info_res);
            const auto & ticket_info = ticket_info_res.ValueOrDie();
            table = ticket_info->arrow_table;
            should_cancel_ticket = cancel_ticket_after_do_get;
        }
        else
        {
            const String & sql = request.ticket;

            auto query_context = session->makeQueryContext();
            query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
            QueryScope query_scope = QueryScope::create(query_context);

            auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

            bool query_finished = false;
            bool handling_exception = false;
            SCOPE_EXIT({
                if (query_finished)
                    block_io.onFinish();
                else if (!handling_exception)
                    block_io.onCancelOrConnectionLoss();
            });

            try
            {
                ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
                ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

                PullingPipelineExecutor executor{block_io.pipeline};

                Block block;
                while (executor.pull(block))
                    chunks.emplace_back(Chunk(block.getColumns(), block.rows()));

                auto header = executor.getHeader();
                auto ch_to_arrow_converter = createCHToArrowConverter(header, query_context);
                ch_to_arrow_converter->chChunkToArrowTable(table, chunks, header.columns());

                query_finished = true;
            }
            catch (...)
            {
                handling_exception = true;
                block_io.onException();
                throw;
            }
        }

        auto stream_res = arrow::RecordBatchReader::MakeFromIterator(
            arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>{arrow::TableBatchReader{table}}, table->schema());
        ARROW_RETURN_NOT_OK(stream_res);
        *stream = std::make_unique<arrow::flight::RecordBatchStream>(stream_res.ValueOrDie());

        if (should_cancel_ticket)
            calls_data->cancelTicket(request.ticket);

        LOG_INFO(log, "DoGet succeeded");
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("DoGet", impl);
}


arrow::Status ArrowFlightServer::DoPut(
    const arrow::flight::ServerCallContext & context,
    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer)
{
    auto impl = [&]
    {
        const auto & request = reader->descriptor();
        LOG_INFO(log, "DoPut is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        /// DoPut with CommandPreparedStatementQuery is parameter binding only (no execution).
        if (request.type == arrow::flight::FlightDescriptor::CMD)
        {
            google::protobuf::Any any_msg;
            if (any_msg.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))
                && any_msg.Is<arrow::flight::protocol::sql::CommandPreparedStatementQuery>())
            {
                arrow::flight::protocol::sql::CommandPreparedStatementQuery command;
                if (!any_msg.UnpackTo(&command))
                    return arrow::Status::SerializationError("Deserialization of sql::CommandPreparedStatementQuery failed.");

                const auto & handle = command.prepared_statement_handle();
                LOG_DEBUG(log, "DoPut: binding parameters for prepared statement {}", handle);

                /// Read parameter values incrementally to detect excess data early
                /// and avoid buffering an unbounded stream into memory.
                /// Some clients may send empty batches before the actual data.
                std::shared_ptr<arrow::RecordBatch> params;
                while (true)
                {
                    ARROW_ASSIGN_OR_RAISE(auto chunk, reader->Next())
                    if (!chunk.data)
                        break;
                    if (chunk.data->num_rows() == 0)
                        continue;
                    if (chunk.data->num_rows() > 1)
                        return arrow::Status::NotImplemented(
                            "Multiple parameter sets are not supported (got ", chunk.data->num_rows(), " rows)");

                    params = std::move(chunk.data);

                    /// Drain remaining batches, rejecting if any contain rows.
                    while (true)
                    {
                        ARROW_ASSIGN_OR_RAISE(auto extra, reader->Next())
                        if (!extra.data)
                            break;
                        if (extra.data->num_rows() > 0)
                            return arrow::Status::NotImplemented(
                                "Multiple parameter sets are not supported");
                    }
                    break;
                }

                ARROW_RETURN_NOT_OK(calls_data->bindParameters(handle, auth.getUsername(), std::move(params)));

                /// Return DoPutPreparedStatementResult with the same handle.
                arrow::flight::protocol::sql::DoPutPreparedStatementResult result;
                result.set_prepared_statement_handle(handle);
                ARROW_RETURN_NOT_OK(writer->WriteMetadata(*arrow::Buffer::FromString(result.SerializeAsString())));

                LOG_INFO(log, "DoPut: parameter binding succeeded for prepared statement {}", handle);
                return arrow::Status::OK();
            }
        }

        bool dont_write_flight_sql_metadata = !ArrowFlight::flightDescriptorIsArrowFlightSqlCommand(request);

        std::string sql;
        ArrowFlight::SchemaModifier schema_modifier;
        ArrowFlight::BlockModifier block_modifier;
        std::shared_ptr<arrow::Table> table;

        ARROW_ASSIGN_OR_RAISE(std::tie(sql, schema_modifier, block_modifier, table), decodeDescriptor(request, true, auth.getUsername()))
        /// DoPut command should only produce sql query
        chassert(!sql.empty() && !schema_modifier && !block_modifier && !table);

        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
        QueryScope query_scope = QueryScope::create(query_context);

        auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

        bool query_finished = false;
        bool handling_exception = false;
        SCOPE_EXIT({
            if (query_finished)
                block_io.onFinish();
            else if (!handling_exception)
                block_io.onCancelOrConnectionLoss();
        });

        try
        {
            ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
            auto & pipeline = block_io.pipeline;

            if (pipeline.pushing())
            {
                Block header = pipeline.getHeader();
                auto input = std::make_shared<ArrowFlightSource>(std::move(reader), header, query_context);
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
            arrow::flight::protocol::sql::DoPutUpdateResult update_result;
            if (auto element = query_context->getProcessListElement())
                update_result.set_record_count(element->getInfo().written_rows);
            else
                update_result.set_record_count(0);

            ARROW_RETURN_NOT_OK(writer->WriteMetadata(*arrow::Buffer::FromString(update_result.SerializeAsString())));
        }

        LOG_INFO(log, "DoPut succeeded");

        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("DoPut", impl);
}


arrow::Status ArrowFlightServer::tryRunAndLogIfError(std::string_view method_name, std::function<arrow::Status()> && func) const
{
    DB::setThreadName(ThreadName::ARROW_FLIGHT);
    ThreadStatus thread_status;
    try
    {
        auto status = std::move(func)();
        if (!status.ok())
            LOG_ERROR(log, "{} failed: {}", method_name, status.ToString());
        return status;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("{} failed", method_name));
        return arrow::Status::ExecutionError(method_name, " failed: ", getCurrentExceptionMessage(/* with_stacktrace = */ false));
    }
}


arrow::Status ArrowFlightServer::DoAction(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::Action & action,
    std::unique_ptr<arrow::flight::ResultStream> * result_stream)
{
    auto impl = [&]
    {
        LOG_INFO(log, "DoAction is called for action {} {}", action.type, action.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        std::vector<arrow::flight::Result> results;

        if (action.type == arrow::flight::ActionType::kCancelFlightInfo.type)
        {
            if (!action.body)
                return arrow::Status::Invalid("Invalid empty CancelFlightInfo action.");
            ARROW_ASSIGN_OR_RAISE(auto request, arrow::flight::CancelFlightInfoRequest::Deserialize({action.body->data_as<char>(), static_cast<size_t>(action.body->size())}))
            LOG_DEBUG(log, "CancelFlightInfo request {}", request.ToString());
            auto query_id = request.info->app_metadata();
            auto result = arrow::flight::CancelFlightInfoResult{arrow::flight::CancelStatus::kNotCancellable};

            if (!query_id.empty())
            {
                auto & process_list = server.context()->getProcessList();
                auto cancel_result = process_list.sendCancelToQuery(query_id, auth.getUsername());
                if (cancel_result == CancellationCode::CancelSent)
                {
                    result = arrow::flight::CancelFlightInfoResult{arrow::flight::CancelStatus::kCancelled};

                    for (const auto & pd : calls_data->collectPollDescriptorsForQueryId(query_id))
                        calls_data->cancelPollDescriptor(pd);
                }
            }

            ARROW_ASSIGN_OR_RAISE(auto serialized, result.SerializeToString())
            ARROW_ASSIGN_OR_RAISE(auto packed_result, arrow::Result<arrow::flight::Result>{arrow::flight::Result{arrow::Buffer::FromString(std::move(serialized))}})
            results.push_back(std::move(packed_result));
        }
        else if (action.type == arrow::flight::ActionType::kSetSessionOptions.type)
        {
            if (!action.body)
                return arrow::Status::Invalid("Invalid empty SetSessionOptions action.");
            ARROW_ASSIGN_OR_RAISE(auto request, arrow::flight::SetSessionOptionsRequest::Deserialize({action.body->data_as<char>(), static_cast<size_t>(action.body->size())}))
            arrow::flight::SetSessionOptionsResult result;

            auto query_context = session->makeQueryContext();
            auto session_context = query_context->getSessionContext();

            /// Convert Arrow Flight SessionOptionValue to a string representation
            /// suitable for Context::setSetting().
            auto to_string_value = overloaded {
                [](const std::string & str) { return str; },
                [](bool b) { return std::string(b ? "true" : "false"); },
                [](int64_t v) { return std::to_string(v); },
                [](double v) { return fmt::format("{}", v); },
                [](const std::vector<std::string> & strings)
                {
                    std::string res = "[";
                    for (size_t i = 0; i < strings.size(); ++i)
                    {
                        if (i > 0) res += ",";
                        res += quoteString(strings[i]);
                    }
                    res += "]";
                    return res;
                },
                /// std::monostate is deliberately excluded here — it means "reset to default"
                /// and is handled separately instead of calling this visitor.
                [](const std::monostate &) -> std::string
                {
                    chassert(false && "std::monostate should be handled separately instead of calling this visitor");
                    return "";
                }
            };

            for (const auto & [setting, value] : request.session_options)
            {
                if (!isValidIdentifier(setting))
                {
                    result.errors[setting] = arrow::flight::SetSessionOptionsResult::Error{
                        arrow::flight::SetSessionOptionErrorValue::kInvalidName
                    };
                    continue;
                }

                try
                {
                    if (std::holds_alternative<std::monostate>(value))
                    {
                        /// std::monostate means "reset to default" (SET setting = DEFAULT).
                        session_context->resetSettingsToDefaultValue({setting});
                    }
                    else
                    {
                        auto string_value = std::visit(to_string_value, value);
                        SettingChange change{setting, Field{string_value}};
                        query_context->checkSettingsConstraints(change, SettingSource::QUERY);
                        session_context->setSetting(setting, string_value);
                    }
                }
                catch (DB::Exception & e)
                {
                    auto error_value = [&]()
                    {
                        if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED || e.code() == ErrorCodes::SYNTAX_ERROR)
                            return arrow::flight::SetSessionOptionErrorValue::kInvalidValue;
                        else if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                            return arrow::flight::SetSessionOptionErrorValue::kInvalidName;
                        else
                            return arrow::flight::SetSessionOptionErrorValue::kUnspecified;
                    }();

                    result.errors[setting] = arrow::flight::SetSessionOptionsResult::Error{error_value};
                }
            }

            ARROW_ASSIGN_OR_RAISE(auto serialized, result.SerializeToString())
            ARROW_ASSIGN_OR_RAISE(auto packed_result, arrow::Result<arrow::flight::Result>{arrow::flight::Result{arrow::Buffer::FromString(std::move(serialized))}})

            results.push_back(std::move(packed_result));
        }
        else if (action.type == arrow::flight::ActionType::kGetSessionOptions.type)
        {
            std::string_view body_view = action.body
                ? std::string_view{action.body->data_as<char>(), static_cast<size_t>(action.body->size())}
                : std::string_view{};
            ARROW_RETURN_NOT_OK(arrow::flight::GetSessionOptionsRequest::Deserialize(body_view));
            arrow::flight::GetSessionOptionsResult result;

            auto execute_res = executeSQLtoTable(session, "SELECT name, value FROM system.settings");
            ARROW_RETURN_NOT_OK(execute_res);
            auto [_, table] = execute_res.ValueUnsafe();
            const auto & names = table->column(0);
            const auto & values = table->column(1);

            if (names->num_chunks() != values->num_chunks())
                return arrow::Status::Invalid("Unexpected chunk layout mismatch for settings columns");

            for (int chunk_idx = 0; chunk_idx < names->num_chunks(); ++chunk_idx)
            {
                const auto & name_chunk_any = names->chunk(chunk_idx);
                const auto & value_chunk_any = values->chunk(chunk_idx);

                if (name_chunk_any->type_id() != arrow::Type::STRING || value_chunk_any->type_id() != arrow::Type::STRING)
                    return arrow::Status::TypeError("Expected STRING chunks in settings result");

                if (name_chunk_any->length() != value_chunk_any->length())
                    return arrow::Status::Invalid("Mismatched chunk lengths for settings columns");

                const auto & name_chunk = static_cast<const arrow::StringArray &>(*name_chunk_any);
                const auto & value_chunk = static_cast<const arrow::StringArray &>(*value_chunk_any);

                for (int64_t i = 0; i < name_chunk.length(); ++i)
                {
                    if (name_chunk.IsNull(i) || value_chunk.IsNull(i))
                        continue;
                    result.session_options[name_chunk.GetString(i)] = value_chunk.GetString(i);
                }
            }

            ARROW_ASSIGN_OR_RAISE(auto serialized, result.SerializeToString())
            ARROW_ASSIGN_OR_RAISE(auto packed_result, arrow::Result<arrow::flight::Result>{arrow::flight::Result{arrow::Buffer::FromString(std::move(serialized))}})

            results.push_back(std::move(packed_result));
        }
        else if (action.type == "CreatePreparedStatement")
        {
            if (!action.body)
                return arrow::Status::Invalid("Invalid empty CreatePreparedStatement action.");

            arrow::flight::protocol::sql::ActionCreatePreparedStatementRequest request;
            if (!request.ParseFromArray(action.body->data(), static_cast<int>(action.body->size())))
                return arrow::Status::Invalid("Could not deserialize ActionCreatePreparedStatementRequest.");

            if (request.has_transaction_id())
                return arrow::Status::NotImplemented("CreatePreparedStatement: transaction_id is not supported");

            const auto & query = request.query();

            if (query.empty())
                return arrow::Status::Invalid("CreatePreparedStatement: query must not be empty");

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
            ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));

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
                            info.dataset_schema = CHColumnToArrowColumn::calculateArrowSchema(
                                executor.getHeader().getColumnsWithTypeAndName(),
                                "Arrow",
                                nullptr,
                                {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]});
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

            ARROW_ASSIGN_OR_RAISE(auto handle, calls_data->createPreparedStatement(std::move(info), auth.getSessionId(), session_timeout_for_ps))

            /// Build the protobuf result.
            arrow::flight::protocol::sql::ActionCreatePreparedStatementResult result;
            result.set_prepared_statement_handle(handle);

            if (auto ps_res = calls_data->getPreparedStatement(handle, auth.getUsername()); ps_res.ok())
            {
                const auto & ps_info = ps_res.ValueUnsafe();
                if (ps_info.dataset_schema)
                {
                    auto serialized_schema = arrow::ipc::SerializeSchema(*ps_info.dataset_schema, arrow::default_memory_pool());
                    if (serialized_schema.ok())
                        result.set_dataset_schema(serialized_schema.ValueUnsafe()->data(), serialized_schema.ValueUnsafe()->size());
                }

                /// Build parameter schema: one field per '?' placeholder.
                /// Type is null because ClickHouse accepts any type and parses the value as a SQL literal.
                arrow::FieldVector param_fields;
                size_t num_params = ps_info.numParams();
                param_fields.reserve(num_params);
                for (size_t i = 0; i < num_params; ++i)
                    param_fields.push_back(arrow::field("parameter_" + std::to_string(i + 1), arrow::null(), /* nullable = */ true));
                auto param_schema = arrow::schema(std::move(param_fields));
                auto serialized_param_schema = arrow::ipc::SerializeSchema(*param_schema, arrow::default_memory_pool());
                if (serialized_param_schema.ok())
                    result.set_parameter_schema(serialized_param_schema.ValueUnsafe()->data(), serialized_param_schema.ValueUnsafe()->size());
            }

            ARROW_ASSIGN_OR_RAISE(auto packed_result, arrow::Result<arrow::flight::Result>{arrow::flight::Result{arrow::Buffer::FromString(result.SerializeAsString())}})
            results.push_back(std::move(packed_result));
        }
        else if (action.type == "ClosePreparedStatement")
        {
            if (!action.body)
                return arrow::Status::Invalid("Invalid empty ClosePreparedStatement action.");

            arrow::flight::protocol::sql::ActionClosePreparedStatementRequest request;
            if (!request.ParseFromArray(action.body->data(), static_cast<int>(action.body->size())))
                return arrow::Status::Invalid("Could not deserialize ActionClosePreparedStatementRequest.");

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
            return arrow::Status::NotImplemented(action.type, " is not supported");

        *result_stream = std::make_unique<arrow::flight::SimpleResultStream>(std::move(results));
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("DoAction", impl);
}

}

#endif
