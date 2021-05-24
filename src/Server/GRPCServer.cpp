#include "GRPCServer.h"
#if USE_GRPC

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/CurrentThread.h>
#include <Common/SettingsChanges.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/executeQuery.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ParserQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Server/IServer.h>
#include <Storages/IStorage.h>
#include <Poco/FileStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <ext/range.h>
#include <grpc++/security/server_credentials.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>


using GRPCService = clickhouse::grpc::ClickHouse::AsyncService;
using GRPCQueryInfo = clickhouse::grpc::QueryInfo;
using GRPCResult = clickhouse::grpc::Result;
using GRPCException = clickhouse::grpc::Exception;
using GRPCProgress = clickhouse::grpc::Progress;

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int INVALID_GRPC_QUERY_INFO;
    extern const int INVALID_SESSION_TIMEOUT;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int NO_DATA_TO_INSERT;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNKNOWN_DATABASE;
}

namespace
{
    /// Make grpc to pass logging messages to ClickHouse logging system.
    void initGRPCLogging(const Poco::Util::AbstractConfiguration & config)
    {
        static std::once_flag once_flag;
        std::call_once(once_flag, [&config]
        {
            static Poco::Logger * logger = &Poco::Logger::get("grpc");
            gpr_set_log_function([](gpr_log_func_args* args)
            {
                if (args->severity == GPR_LOG_SEVERITY_DEBUG)
                    LOG_DEBUG(logger, "{} ({}:{})", args->message, args->file, args->line);
                else if (args->severity == GPR_LOG_SEVERITY_INFO)
                    LOG_INFO(logger, "{} ({}:{})", args->message, args->file, args->line);
                else if (args->severity == GPR_LOG_SEVERITY_ERROR)
                    LOG_ERROR(logger, "{} ({}:{})", args->message, args->file, args->line);
            });

            if (config.getBool("grpc.verbose_logs", false))
            {
                gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);
                grpc_tracer_set_enabled("all", true);
            }
            else if (logger->is(Poco::Message::PRIO_DEBUG))
            {
                gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);
            }
            else if (logger->is(Poco::Message::PRIO_INFORMATION))
            {
                gpr_set_log_verbosity(GPR_LOG_SEVERITY_INFO);
            }
        });
    }

    grpc_compression_algorithm parseCompressionAlgorithm(const String & str)
    {
        if (str == "none")
            return GRPC_COMPRESS_NONE;
        else if (str == "deflate")
            return GRPC_COMPRESS_DEFLATE;
        else if (str == "gzip")
            return GRPC_COMPRESS_GZIP;
        else if (str == "stream_gzip")
            return GRPC_COMPRESS_STREAM_GZIP;
        else
            throw Exception("Unknown compression algorithm: '" + str + "'", ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    grpc_compression_level parseCompressionLevel(const String & str)
    {
        if (str == "none")
            return GRPC_COMPRESS_LEVEL_NONE;
        else if (str == "low")
            return GRPC_COMPRESS_LEVEL_LOW;
        else if (str == "medium")
            return GRPC_COMPRESS_LEVEL_MED;
        else if (str == "high")
            return GRPC_COMPRESS_LEVEL_HIGH;
        else
            throw Exception("Unknown compression level: '" + str + "'", ErrorCodes::INVALID_CONFIG_PARAMETER);
    }


    /// Gets file's contents as a string, throws an exception if failed.
    String readFile(const String & filepath)
    {
        Poco::FileInputStream ifs(filepath);
        String res;
        Poco::StreamCopier::copyToString(ifs, res);
        return res;
    }

    /// Makes credentials based on the server config.
    std::shared_ptr<grpc::ServerCredentials> makeCredentials(const Poco::Util::AbstractConfiguration & config)
    {
        if (config.getBool("grpc.enable_ssl", false))
        {
#if USE_SSL
            grpc::SslServerCredentialsOptions options;
            grpc::SslServerCredentialsOptions::PemKeyCertPair key_cert_pair;
            key_cert_pair.private_key = readFile(config.getString("grpc.ssl_key_file"));
            key_cert_pair.cert_chain = readFile(config.getString("grpc.ssl_cert_file"));
            options.pem_key_cert_pairs.emplace_back(std::move(key_cert_pair));
            if (config.getBool("grpc.ssl_require_client_auth", false))
            {
                options.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
                if (config.has("grpc.ssl_ca_cert_file"))
                    options.pem_root_certs = readFile(config.getString("grpc.ssl_ca_cert_file"));
            }
            return grpc::SslServerCredentials(options);
#else
            throw DB::Exception(
                "Can't use SSL in grpc, because ClickHouse was built without SSL library",
                DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
        }
        return grpc::InsecureServerCredentials();
    }


    /// Gets session's timeout from query info or from the server config.
    std::chrono::steady_clock::duration getSessionTimeout(const GRPCQueryInfo & query_info, const Poco::Util::AbstractConfiguration & config)
    {
        auto session_timeout = query_info.session_timeout();
        if (session_timeout)
        {
            auto max_session_timeout = config.getUInt("max_session_timeout", 3600);
            if (session_timeout > max_session_timeout)
                throw Exception(
                    "Session timeout '" + std::to_string(session_timeout) + "' is larger than max_session_timeout: "
                        + std::to_string(max_session_timeout) + ". Maximum session timeout could be modified in configuration file.",
                    ErrorCodes::INVALID_SESSION_TIMEOUT);
        }
        else
            session_timeout = config.getInt("default_session_timeout", 60);
        return std::chrono::seconds(session_timeout);
    }

    /// Generates a description of a query by a specified query info.
    /// This description is used for logging only.
    String getQueryDescription(const GRPCQueryInfo & query_info)
    {
        String str;
        if (!query_info.query().empty())
        {
            std::string_view query = query_info.query();
            constexpr size_t max_query_length_to_log = 64;
            if (query.length() > max_query_length_to_log)
                query.remove_suffix(query.length() - max_query_length_to_log);
            if (size_t format_pos = query.find(" FORMAT "); format_pos != String::npos)
                query.remove_suffix(query.length() - format_pos - strlen(" FORMAT "));
            str.append("\"").append(query);
            if (query != query_info.query())
                str.append("...");
            str.append("\"");
        }
        if (!query_info.query_id().empty())
            str.append(str.empty() ? "" : ", ").append("query_id: ").append(query_info.query_id());
        if (!query_info.input_data().empty())
            str.append(str.empty() ? "" : ", ").append("input_data: ").append(std::to_string(query_info.input_data().size())).append(" bytes");
        if (query_info.external_tables_size())
            str.append(str.empty() ? "" : ", ").append("external tables: ").append(std::to_string(query_info.external_tables_size()));
        return str;
    }

    /// Generates a description of a result.
    /// This description is used for logging only.
    String getResultDescription(const GRPCResult & result)
    {
        String str;
        if (!result.output().empty())
            str.append("output: ").append(std::to_string(result.output().size())).append(" bytes");
        if (!result.totals().empty())
            str.append(str.empty() ? "" : ", ").append("totals");
        if (!result.extremes().empty())
            str.append(str.empty() ? "" : ", ").append("extremes");
        if (result.has_progress())
            str.append(str.empty() ? "" : ", ").append("progress");
        if (result.logs_size())
            str.append(str.empty() ? "" : ", ").append("logs: ").append(std::to_string(result.logs_size())).append(" entries");
        if (result.cancelled())
            str.append(str.empty() ? "" : ", ").append("cancelled");
        if (result.has_exception())
            str.append(str.empty() ? "" : ", ").append("exception");
        return str;
    }

    using CompletionCallback = std::function<void(bool)>;

    /// Requests a connection and provides low-level interface for reading and writing.
    class BaseResponder
    {
    public:
        virtual ~BaseResponder() = default;

        virtual void start(GRPCService & grpc_service,
                           grpc::ServerCompletionQueue & new_call_queue,
                           grpc::ServerCompletionQueue & notification_queue,
                           const CompletionCallback & callback) = 0;

        virtual void read(GRPCQueryInfo & query_info_, const CompletionCallback & callback) = 0;
        virtual void write(const GRPCResult & result, const CompletionCallback & callback) = 0;
        virtual void writeAndFinish(const GRPCResult & result, const grpc::Status & status, const CompletionCallback & callback) = 0;

        Poco::Net::SocketAddress getClientAddress() const { String peer = grpc_context.peer(); return Poco::Net::SocketAddress{peer.substr(peer.find(':') + 1)}; }

    protected:
        CompletionCallback * getCallbackPtr(const CompletionCallback & callback)
        {
            /// It would be better to pass callbacks to gRPC calls.
            /// However gRPC calls can be tagged with `void *` tags only.
            /// The map `callbacks` here is used to keep callbacks until they're called.
            std::lock_guard lock{mutex};
            size_t callback_id = next_callback_id++;
            auto & callback_in_map = callbacks[callback_id];
            callback_in_map = [this, callback, callback_id](bool ok)
            {
                CompletionCallback callback_to_call;
                {
                    std::lock_guard lock2{mutex};
                    callback_to_call = callback;
                    callbacks.erase(callback_id);
                }
                callback_to_call(ok);
            };
            return &callback_in_map;
        }

        grpc::ServerContext grpc_context;

    private:
        grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryInfo> reader_writer{&grpc_context};
        std::unordered_map<size_t, CompletionCallback> callbacks;
        size_t next_callback_id = 0;
        std::mutex mutex;
    };

    enum CallType
    {
        CALL_SIMPLE,             /// ExecuteQuery() call
        CALL_WITH_STREAM_INPUT,  /// ExecuteQueryWithStreamInput() call
        CALL_WITH_STREAM_OUTPUT, /// ExecuteQueryWithStreamOutput() call
        CALL_WITH_STREAM_IO,     /// ExecuteQueryWithStreamIO() call
        CALL_MAX,
    };

    const char * getCallName(CallType call_type)
    {
        switch (call_type)
        {
            case CALL_SIMPLE: return "ExecuteQuery()";
            case CALL_WITH_STREAM_INPUT: return "ExecuteQueryWithStreamInput()";
            case CALL_WITH_STREAM_OUTPUT: return "ExecuteQueryWithStreamOutput()";
            case CALL_WITH_STREAM_IO: return "ExecuteQueryWithStreamIO()";
            case CALL_MAX: break;
        }
        __builtin_unreachable();
    }

    bool isInputStreaming(CallType call_type)
    {
        return (call_type == CALL_WITH_STREAM_INPUT) || (call_type == CALL_WITH_STREAM_IO);
    }

    bool isOutputStreaming(CallType call_type)
    {
        return (call_type == CALL_WITH_STREAM_OUTPUT) || (call_type == CALL_WITH_STREAM_IO);
    }

    template <enum CallType call_type>
    class Responder;

    template<>
    class Responder<CALL_SIMPLE> : public BaseResponder
    {
    public:
        void start(GRPCService & grpc_service,
                  grpc::ServerCompletionQueue & new_call_queue,
                  grpc::ServerCompletionQueue & notification_queue,
                  const CompletionCallback & callback) override
        {
            grpc_service.RequestExecuteQuery(&grpc_context, &query_info.emplace(), &response_writer, &new_call_queue, &notification_queue, getCallbackPtr(callback));
        }

        void read(GRPCQueryInfo & query_info_, const CompletionCallback & callback) override
        {
            if (!query_info.has_value())
                callback(false);
            query_info_ = std::move(query_info).value();
            query_info.reset();
            callback(true);
        }

        void write(const GRPCResult &, const CompletionCallback &) override
        {
            throw Exception("Responder<CALL_SIMPLE>::write() should not be called", ErrorCodes::LOGICAL_ERROR);
        }

        void writeAndFinish(const GRPCResult & result, const grpc::Status & status, const CompletionCallback & callback) override
        {
            response_writer.Finish(result, status, getCallbackPtr(callback));
        }

    private:
        grpc::ServerAsyncResponseWriter<GRPCResult> response_writer{&grpc_context};
        std::optional<GRPCQueryInfo> query_info;
    };

    template<>
    class Responder<CALL_WITH_STREAM_INPUT> : public BaseResponder
    {
    public:
        void start(GRPCService & grpc_service,
                  grpc::ServerCompletionQueue & new_call_queue,
                  grpc::ServerCompletionQueue & notification_queue,
                  const CompletionCallback & callback) override
        {
            grpc_service.RequestExecuteQueryWithStreamInput(&grpc_context, &reader, &new_call_queue, &notification_queue, getCallbackPtr(callback));
        }

        void read(GRPCQueryInfo & query_info_, const CompletionCallback & callback) override
        {
            reader.Read(&query_info_, getCallbackPtr(callback));
        }

        void write(const GRPCResult &, const CompletionCallback &) override
        {
            throw Exception("Responder<CALL_WITH_STREAM_INPUT>::write() should not be called", ErrorCodes::LOGICAL_ERROR);
        }

        void writeAndFinish(const GRPCResult & result, const grpc::Status & status, const CompletionCallback & callback) override
        {
            reader.Finish(result, status, getCallbackPtr(callback));
        }

    private:
        grpc::ServerAsyncReader<GRPCResult, GRPCQueryInfo> reader{&grpc_context};
    };

    template<>
    class Responder<CALL_WITH_STREAM_OUTPUT> : public BaseResponder
    {
    public:
        void start(GRPCService & grpc_service,
                  grpc::ServerCompletionQueue & new_call_queue,
                  grpc::ServerCompletionQueue & notification_queue,
                  const CompletionCallback & callback) override
        {
            grpc_service.RequestExecuteQueryWithStreamOutput(&grpc_context, &query_info.emplace(), &writer, &new_call_queue, &notification_queue, getCallbackPtr(callback));
        }

        void read(GRPCQueryInfo & query_info_, const CompletionCallback & callback) override
        {
            if (!query_info.has_value())
                callback(false);
            query_info_ = std::move(query_info).value();
            query_info.reset();
            callback(true);
        }

        void write(const GRPCResult & result, const CompletionCallback & callback) override
        {
            writer.Write(result, getCallbackPtr(callback));
        }

        void writeAndFinish(const GRPCResult & result, const grpc::Status & status, const CompletionCallback & callback) override
        {
            writer.WriteAndFinish(result, {}, status, getCallbackPtr(callback));
        }

    private:
        grpc::ServerAsyncWriter<GRPCResult> writer{&grpc_context};
        std::optional<GRPCQueryInfo> query_info;
    };

    template<>
    class Responder<CALL_WITH_STREAM_IO> : public BaseResponder
    {
    public:
        void start(GRPCService & grpc_service,
                  grpc::ServerCompletionQueue & new_call_queue,
                  grpc::ServerCompletionQueue & notification_queue,
                  const CompletionCallback & callback) override
        {
            grpc_service.RequestExecuteQueryWithStreamIO(&grpc_context, &reader_writer, &new_call_queue, &notification_queue, getCallbackPtr(callback));
        }

        void read(GRPCQueryInfo & query_info_, const CompletionCallback & callback) override
        {
            reader_writer.Read(&query_info_, getCallbackPtr(callback));
        }

        void write(const GRPCResult & result, const CompletionCallback & callback) override
        {
            reader_writer.Write(result, getCallbackPtr(callback));
        }

        void writeAndFinish(const GRPCResult & result, const grpc::Status & status, const CompletionCallback & callback) override
        {
            reader_writer.WriteAndFinish(result, {}, status, getCallbackPtr(callback));
        }

    private:
        grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryInfo> reader_writer{&grpc_context};
    };

    std::unique_ptr<BaseResponder> makeResponder(CallType call_type)
    {
        switch (call_type)
        {
            case CALL_SIMPLE: return std::make_unique<Responder<CALL_SIMPLE>>();
            case CALL_WITH_STREAM_INPUT: return std::make_unique<Responder<CALL_WITH_STREAM_INPUT>>();
            case CALL_WITH_STREAM_OUTPUT: return std::make_unique<Responder<CALL_WITH_STREAM_OUTPUT>>();
            case CALL_WITH_STREAM_IO: return std::make_unique<Responder<CALL_WITH_STREAM_IO>>();
            case CALL_MAX: break;
        }
        __builtin_unreachable();
    }


    /// Implementation of ReadBuffer, which just calls a callback.
    class ReadBufferFromCallback : public ReadBuffer
    {
    public:
        explicit ReadBufferFromCallback(const std::function<std::pair<const void *, size_t>(void)> & callback_)
            : ReadBuffer(nullptr, 0), callback(callback_) {}

    private:
        bool nextImpl() override
        {
            const void * new_pos;
            size_t new_size;
            std::tie(new_pos, new_size) = callback();
            if (!new_size)
                return false;
            BufferBase::set(static_cast<BufferBase::Position>(const_cast<void *>(new_pos)), new_size, 0);
            return true;
        }

        std::function<std::pair<const void *, size_t>(void)> callback;
    };


    /// Handles a connection after a responder is started (i.e. after getting a new call).
    class Call
    {
    public:
        Call(CallType call_type_, std::unique_ptr<BaseResponder> responder_, IServer & iserver_, Poco::Logger * log_);
        ~Call();

        void start(const std::function<void(void)> & on_finish_call_callback);

    private:
        void run();

        void receiveQuery();
        void executeQuery();

        void processInput();
        void initializeBlockInputStream(const Block & header);
        void createExternalTables();

        void generateOutput();
        void generateOutputWithProcessors();

        void finishQuery();
        void onException(const Exception & exception);
        void onFatalError();
        void close();

        void readQueryInfo();
        void throwIfFailedToReadQueryInfo();
        bool isQueryCancelled();

        void addProgressToResult();
        void addTotalsToResult(const Block & totals);
        void addExtremesToResult(const Block & extremes);
        void addProfileInfoToResult(const BlockStreamProfileInfo & info);
        void addLogsToResult();
        void sendResult();
        void throwIfFailedToSendResult();
        void sendException(const Exception & exception);

        const CallType call_type;
        std::unique_ptr<BaseResponder> responder;
        IServer & iserver;
        Poco::Logger * log = nullptr;

        std::shared_ptr<NamedSession> session;
        ContextPtr query_context;
        std::optional<CurrentThread::QueryScope> query_scope;
        String query_text;
        ASTPtr ast;
        ASTInsertQuery * insert_query = nullptr;
        String input_format;
        String input_data_delimiter;
        String output_format;
        uint64_t interactive_delay = 100000;
        bool send_exception_with_stacktrace = true;
        bool input_function_is_used = false;

        BlockIO io;
        Progress progress;
        InternalTextLogsQueuePtr logs_queue;

        GRPCQueryInfo query_info; /// We reuse the same messages multiple times.
        GRPCResult result;

        bool initial_query_info_read = false;
        bool finalize = false;
        bool responder_finished = false;
        bool cancelled = false;

        std::optional<ReadBufferFromCallback> read_buffer;
        std::optional<WriteBufferFromString> write_buffer;
        BlockInputStreamPtr block_input_stream;
        BlockOutputStreamPtr block_output_stream;
        bool need_input_data_from_insert_query = true;
        bool need_input_data_from_query_info = true;
        bool need_input_data_delimiter = false;

        Stopwatch query_time;
        UInt64 waited_for_client_reading = 0;
        UInt64 waited_for_client_writing = 0;

        /// The following fields are accessed both from call_thread and queue_thread.
        std::atomic<bool> reading_query_info = false;
        std::atomic<bool> failed_to_read_query_info = false;
        GRPCQueryInfo next_query_info_while_reading;
        std::atomic<bool> want_to_cancel = false;
        std::atomic<bool> check_query_info_contains_cancel_only = false;
        std::atomic<bool> sending_result = false;
        std::atomic<bool> failed_to_send_result = false;

        ThreadFromGlobalPool call_thread;
        std::condition_variable read_finished;
        std::condition_variable write_finished;
        std::mutex dummy_mutex; /// Doesn't protect anything.
    };

    Call::Call(CallType call_type_, std::unique_ptr<BaseResponder> responder_, IServer & iserver_, Poco::Logger * log_)
        : call_type(call_type_), responder(std::move(responder_)), iserver(iserver_), log(log_)
    {
    }

    Call::~Call()
    {
        if (call_thread.joinable())
            call_thread.join();
    }

    void Call::start(const std::function<void(void)> & on_finish_call_callback)
    {
        auto runner_function = [this, on_finish_call_callback]
        {
            try
            {
                run();
            }
            catch (...)
            {
                tryLogCurrentException("GRPCServer");
            }
            on_finish_call_callback();
        };
        call_thread = ThreadFromGlobalPool(runner_function);
    }

    void Call::run()
    {
        try
        {
            receiveQuery();
            executeQuery();
            processInput();
            generateOutput();
            finishQuery();
        }
        catch (Exception & exception)
        {
            onException(exception);
        }
        catch (Poco::Exception & exception)
        {
            onException(Exception{Exception::CreateFromPocoTag{}, exception});
        }
        catch (std::exception & exception)
        {
            onException(Exception{Exception::CreateFromSTDTag{}, exception});
        }
    }

    void Call::receiveQuery()
    {
        LOG_INFO(log, "Handling call {}", getCallName(call_type));

        readQueryInfo();

        if (query_info.cancel())
            throw Exception("Initial query info cannot set the 'cancel' field", ErrorCodes::INVALID_GRPC_QUERY_INFO);

        LOG_DEBUG(log, "Received initial QueryInfo: {}", getQueryDescription(query_info));
    }

    void Call::executeQuery()
    {
        /// Retrieve user credentials.
        std::string user = query_info.user_name();
        std::string password = query_info.password();
        std::string quota_key = query_info.quota();
        Poco::Net::SocketAddress user_address = responder->getClientAddress();

        if (user.empty())
        {
            user = "default";
            password = "";
        }

        /// Create context.
        query_context = Context::createCopy(iserver.context());

        /// Authentication.
        query_context->setUser(user, password, user_address);
        query_context->setCurrentQueryId(query_info.query_id());
        if (!quota_key.empty())
            query_context->setQuotaKey(quota_key);

        /// The user could specify session identifier and session timeout.
        /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.
        if (!query_info.session_id().empty())
        {
            session = query_context->acquireNamedSession(
                query_info.session_id(), getSessionTimeout(query_info, iserver.config()), query_info.session_check());
            query_context = Context::createCopy(session->context);
            query_context->setSessionContext(session->context);
        }

        query_scope.emplace(query_context);

        /// Set client info.
        ClientInfo & client_info = query_context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;
        client_info.initial_user = client_info.current_user;
        client_info.initial_query_id = client_info.current_query_id;
        client_info.initial_address = client_info.current_address;

        /// Prepare settings.
        SettingsChanges settings_changes;
        for (const auto & [key, value] : query_info.settings())
        {
            settings_changes.push_back({key, value});
        }
        query_context->checkSettingsConstraints(settings_changes);
        query_context->applySettingsChanges(settings_changes);
        const Settings & settings = query_context->getSettingsRef();

        /// Prepare for sending exceptions and logs.
        send_exception_with_stacktrace = query_context->getSettingsRef().calculate_text_stack_trace;
        const auto client_logs_level = query_context->getSettingsRef().send_logs_level;
        if (client_logs_level != LogsLevel::none)
        {
            logs_queue = std::make_shared<InternalTextLogsQueue>();
            logs_queue->max_priority = Poco::Logger::parseLevel(client_logs_level.toString());
            CurrentThread::attachInternalTextLogsQueue(logs_queue, client_logs_level);
            CurrentThread::setFatalErrorCallback([this]{ onFatalError(); });
        }

        /// Set the current database if specified.
        if (!query_info.database().empty())
        {
            if (!DatabaseCatalog::instance().isDatabaseExist(query_info.database()))
                throw Exception("Database " + query_info.database() + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
            query_context->setCurrentDatabase(query_info.database());
        }

        /// The interactive delay will be used to show progress.
        interactive_delay = query_context->getSettingsRef().interactive_delay;
        query_context->setProgressCallback([this](const Progress & value) { return progress.incrementPiecewiseAtomically(value); });

        /// Parse the query.
        query_text = std::move(*(query_info.mutable_query()));
        const char * begin = query_text.data();
        const char * end = begin + query_text.size();
        ParserQuery parser(end);
        ast = parseQuery(parser, begin, end, "", settings.max_query_size, settings.max_parser_depth);

        /// Choose input format.
        insert_query = ast->as<ASTInsertQuery>();
        if (insert_query)
        {
            input_format = insert_query->format;
            if (input_format.empty())
                input_format = "Values";
        }

        input_data_delimiter = query_info.input_data_delimiter();

        /// Choose output format.
        query_context->setDefaultFormat(query_info.output_format());
        if (const auto * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());
            ast_query_with_output && ast_query_with_output->format)
        {
            output_format = getIdentifierName(ast_query_with_output->format);
        }
        if (output_format.empty())
            output_format = query_context->getDefaultFormat();

        /// Set callback to create and fill external tables
        query_context->setExternalTablesInitializer([this] (ContextPtr context)
        {
            if (context != query_context)
                throw Exception("Unexpected context in external tables initializer", ErrorCodes::LOGICAL_ERROR);
            createExternalTables();
        });

        /// Set callbacks to execute function input().
        query_context->setInputInitializer([this] (ContextPtr context, const StoragePtr & input_storage)
        {
            if (context != query_context)
                throw Exception("Unexpected context in Input initializer", ErrorCodes::LOGICAL_ERROR);
            input_function_is_used = true;
            initializeBlockInputStream(input_storage->getInMemoryMetadataPtr()->getSampleBlock());
            block_input_stream->readPrefix();
        });

        query_context->setInputBlocksReaderCallback([this](ContextPtr context) -> Block
        {
            if (context != query_context)
                throw Exception("Unexpected context in InputBlocksReader", ErrorCodes::LOGICAL_ERROR);
            auto block = block_input_stream->read();
            if (!block)
                block_input_stream->readSuffix();
            return block;
        });

        /// Start executing the query.
        const auto * query_end = end;
        if (insert_query && insert_query->data)
        {
            query_end = insert_query->data;
        }
        String query(begin, query_end);
        io = ::DB::executeQuery(query, query_context, false, QueryProcessingStage::Complete, true, true);
    }

    void Call::processInput()
    {
        if (!io.out)
            return;

        bool has_data_to_insert = (insert_query && insert_query->data)
                                  || !query_info.input_data().empty() || query_info.next_query_info();
        if (!has_data_to_insert)
        {
            if (!insert_query)
                throw Exception("Query requires data to insert, but it is not an INSERT query", ErrorCodes::NO_DATA_TO_INSERT);
            else
                throw Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);
        }

        /// This is significant, because parallel parsing may be used.
        /// So we mustn't touch the input stream from other thread.
        initializeBlockInputStream(io.out->getHeader());

        block_input_stream->readPrefix();
        io.out->writePrefix();

        while (auto block = block_input_stream->read())
            io.out->write(block);

        block_input_stream->readSuffix();
        io.out->writeSuffix();
    }

    void Call::initializeBlockInputStream(const Block & header)
    {
        assert(!read_buffer);
        read_buffer.emplace([this]() -> std::pair<const void *, size_t>
        {
            if (need_input_data_from_insert_query)
            {
                need_input_data_from_insert_query = false;
                if (insert_query && insert_query->data && (insert_query->data != insert_query->end))
                {
                    need_input_data_delimiter = !input_data_delimiter.empty();
                    return {insert_query->data, insert_query->end - insert_query->data};
                }
            }

            while (true)
            {
                if (need_input_data_from_query_info)
                {
                    if (need_input_data_delimiter && !query_info.input_data().empty())
                    {
                        need_input_data_delimiter = false;
                        return {input_data_delimiter.data(), input_data_delimiter.size()};
                    }
                    need_input_data_from_query_info = false;
                    if (!query_info.input_data().empty())
                    {
                        need_input_data_delimiter = !input_data_delimiter.empty();
                        return {query_info.input_data().data(), query_info.input_data().size()};
                    }
                }

                if (!query_info.next_query_info())
                    break;

                if (!isInputStreaming(call_type))
                    throw Exception("next_query_info is allowed to be set only for streaming input", ErrorCodes::INVALID_GRPC_QUERY_INFO);

                readQueryInfo();
                if (!query_info.query().empty() || !query_info.query_id().empty() || !query_info.settings().empty()
                    || !query_info.database().empty() || !query_info.input_data_delimiter().empty() || !query_info.output_format().empty()
                    || query_info.external_tables_size() || !query_info.user_name().empty() || !query_info.password().empty()
                    || !query_info.quota().empty() || !query_info.session_id().empty())
                {
                    throw Exception("Extra query infos can be used only to add more input data. "
                                    "Only the following fields can be set: input_data, next_query_info, cancel",
                                    ErrorCodes::INVALID_GRPC_QUERY_INFO);
                }

                if (isQueryCancelled())
                    break;

                LOG_DEBUG(log, "Received extra QueryInfo: input_data: {} bytes", query_info.input_data().size());
                need_input_data_from_query_info = true;
            }

            return {nullptr, 0}; /// no more input data
        });

        assert(!block_input_stream);
        block_input_stream = query_context->getInputFormat(
            input_format, *read_buffer, header, query_context->getSettings().max_insert_block_size);

        /// Add default values if necessary.
        if (ast)
        {
            if (insert_query)
            {
                auto table_id = query_context->resolveStorageID(insert_query->table_id, Context::ResolveOrdinary);
                if (query_context->getSettingsRef().input_format_defaults_for_omitted_fields && table_id)
                {
                    StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, query_context);
                    const auto & columns = storage->getInMemoryMetadataPtr()->getColumns();
                    if (!columns.empty())
                        block_input_stream = std::make_shared<AddingDefaultsBlockInputStream>(block_input_stream, columns, query_context);
                }
            }
        }
    }

    void Call::createExternalTables()
    {
        while (true)
        {
            for (const auto & external_table : query_info.external_tables())
            {
                String name = external_table.name();
                if (name.empty())
                    name = "_data";
                auto temporary_id = StorageID::createEmpty();
                temporary_id.table_name = name;

                /// If such a table does not exist, create it.
                StoragePtr storage;
                if (auto resolved = query_context->tryResolveStorageID(temporary_id, Context::ResolveExternal))
                {
                    storage = DatabaseCatalog::instance().getTable(resolved, query_context);
                }
                else
                {
                    NamesAndTypesList columns;
                    for (size_t column_idx : ext::range(external_table.columns_size()))
                    {
                        const auto & name_and_type = external_table.columns(column_idx);
                        NameAndTypePair column;
                        column.name = name_and_type.name();
                        if (column.name.empty())
                            column.name = "_" + std::to_string(column_idx + 1);
                        column.type = DataTypeFactory::instance().get(name_and_type.type());
                        columns.emplace_back(std::move(column));
                    }
                    auto temporary_table = TemporaryTableHolder(query_context, ColumnsDescription{columns}, {});
                    storage = temporary_table.getTable();
                    query_context->addExternalTable(temporary_id.table_name, std::move(temporary_table));
                }

                if (!external_table.data().empty())
                {
                    /// The data will be written directly to the table.
                    auto metadata_snapshot = storage->getInMemoryMetadataPtr();
                    auto out_stream = storage->write(ASTPtr(), metadata_snapshot, query_context);
                    ReadBufferFromMemory data(external_table.data().data(), external_table.data().size());
                    String format = external_table.format();
                    if (format.empty())
                        format = "TabSeparated";
                    ContextPtr external_table_context = query_context;
                    ContextPtr temp_context;
                    if (!external_table.settings().empty())
                    {
                        temp_context = Context::createCopy(query_context);
                        external_table_context = temp_context;
                        SettingsChanges settings_changes;
                        for (const auto & [key, value] : external_table.settings())
                            settings_changes.push_back({key, value});
                        external_table_context->checkSettingsConstraints(settings_changes);
                        external_table_context->applySettingsChanges(settings_changes);
                    }
                    auto in_stream = external_table_context->getInputFormat(
                        format, data, metadata_snapshot->getSampleBlock(), external_table_context->getSettings().max_insert_block_size);
                    in_stream->readPrefix();
                    out_stream->writePrefix();
                    while (auto block = in_stream->read())
                        out_stream->write(block);
                    in_stream->readSuffix();
                    out_stream->writeSuffix();
                }
            }

            if (!query_info.input_data().empty())
            {
                /// External tables must be created before executing query,
                /// so all external tables must be send no later sending any input data.
                break;
            }

            if (!query_info.next_query_info())
                break;

            if (!isInputStreaming(call_type))
                throw Exception("next_query_info is allowed to be set only for streaming input", ErrorCodes::INVALID_GRPC_QUERY_INFO);

            readQueryInfo();
            if (!query_info.query().empty() || !query_info.query_id().empty() || !query_info.settings().empty()
                || !query_info.database().empty() || !query_info.input_data_delimiter().empty()
                || !query_info.output_format().empty() || !query_info.user_name().empty() || !query_info.password().empty()
                || !query_info.quota().empty() || !query_info.session_id().empty())
            {
                throw Exception("Extra query infos can be used only to add more data to input or more external tables. "
                                "Only the following fields can be set: input_data, external_tables, next_query_info, cancel",
                                ErrorCodes::INVALID_GRPC_QUERY_INFO);
            }
            if (isQueryCancelled())
                break;
            LOG_DEBUG(log, "Received extra QueryInfo: external tables: {}", query_info.external_tables_size());
        }
    }

    void Call::generateOutput()
    {
        if (io.pipeline.initialized())
        {
            generateOutputWithProcessors();
            return;
        }

        if (!io.in)
            return;

        AsynchronousBlockInputStream async_in(io.in);
        write_buffer.emplace(*result.mutable_output());
        block_output_stream = query_context->getOutputStream(output_format, *write_buffer, async_in.getHeader());
        Stopwatch after_send_progress;

        /// Unless the input() function is used we are not going to receive input data anymore.
        if (!input_function_is_used)
            check_query_info_contains_cancel_only = true;

        auto check_for_cancel = [&]
        {
            if (isQueryCancelled())
            {
                async_in.cancel(false);
                return false;
            }
            return true;
        };

        async_in.readPrefix();
        block_output_stream->writePrefix();

        while (check_for_cancel())
        {
            Block block;
            if (async_in.poll(interactive_delay / 1000))
            {
                block = async_in.read();
                if (!block)
                    break;
            }

            throwIfFailedToSendResult();
            if (!check_for_cancel())
                break;

            if (block && !io.null_format)
                block_output_stream->write(block);

            if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
            {
                addProgressToResult();
                after_send_progress.restart();
            }

            addLogsToResult();

            bool has_output = write_buffer->offset();
            if (has_output || result.has_progress() || result.logs_size())
                sendResult();

            throwIfFailedToSendResult();
            if (!check_for_cancel())
                break;
        }

        async_in.readSuffix();
        block_output_stream->writeSuffix();

        if (!isQueryCancelled())
        {
            addTotalsToResult(io.in->getTotals());
            addExtremesToResult(io.in->getExtremes());
            addProfileInfoToResult(io.in->getProfileInfo());
        }
    }

    void Call::generateOutputWithProcessors()
    {
        if (!io.pipeline.initialized())
            return;

        auto executor = std::make_shared<PullingAsyncPipelineExecutor>(io.pipeline);
        write_buffer.emplace(*result.mutable_output());
        block_output_stream = query_context->getOutputStream(output_format, *write_buffer, executor->getHeader());
        block_output_stream->writePrefix();
        Stopwatch after_send_progress;

        /// Unless the input() function is used we are not going to receive input data anymore.
        if (!input_function_is_used)
            check_query_info_contains_cancel_only = true;

        auto check_for_cancel = [&]
        {
            if (isQueryCancelled())
            {
                executor->cancel();
                return false;
            }
            return true;
        };

        Block block;
        while (check_for_cancel())
        {
            if (!executor->pull(block, interactive_delay / 1000))
                break;

            throwIfFailedToSendResult();
            if (!check_for_cancel())
                break;

            if (block && !io.null_format)
                block_output_stream->write(block);

            if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
            {
                addProgressToResult();
                after_send_progress.restart();
            }

            addLogsToResult();

            bool has_output = write_buffer->offset();
            if (has_output || result.has_progress() || result.logs_size())
                sendResult();

            throwIfFailedToSendResult();
            if (!check_for_cancel())
                break;
        }

        block_output_stream->writeSuffix();

        if (!isQueryCancelled())
        {
            addTotalsToResult(executor->getTotalsBlock());
            addExtremesToResult(executor->getExtremesBlock());
            addProfileInfoToResult(executor->getProfileInfo());
        }
    }

    void Call::finishQuery()
    {
        finalize = true;
        io.onFinish();
        addProgressToResult();
        query_scope->logPeakMemoryUsage();
        addLogsToResult();
        sendResult();
        close();

        LOG_INFO(
            log,
            "Finished call {} in {} secs. (including reading by client: {}, writing by client: {})",
            getCallName(call_type),
            query_time.elapsedSeconds(),
            static_cast<double>(waited_for_client_reading) / 1000000000ULL,
            static_cast<double>(waited_for_client_writing) / 1000000000ULL);
    }

    void Call::onException(const Exception & exception)
    {
        io.onException();

        LOG_ERROR(log, "Code: {}, e.displayText() = {}, Stack trace:\n\n{}", exception.code(), exception.displayText(), exception.getStackTraceString());

        if (responder && !responder_finished)
        {
            try
            {
                /// Try to send logs to client, but it could be risky too.
                addLogsToResult();
            }
            catch (...)
            {
                LOG_WARNING(log, "Couldn't send logs to client");
            }

            try
            {
                sendException(exception);
            }
            catch (...)
            {
                LOG_WARNING(log, "Couldn't send exception information to the client");
            }
        }

        close();
    }

    void Call::onFatalError()
    {
        if (responder && !responder_finished)
        {
            try
            {
                finalize = true;
                addLogsToResult();
                sendResult();
            }
            catch (...)
            {
            }
        }
    }

    void Call::close()
    {
        responder.reset();
        block_input_stream.reset();
        block_output_stream.reset();
        read_buffer.reset();
        write_buffer.reset();
        io = {};
        query_scope.reset();
        query_context.reset();
        if (session)
            session->release();
        session.reset();
    }

    void Call::readQueryInfo()
    {
        auto start_reading = [&]
        {
            assert(!reading_query_info);
            reading_query_info = true;
            responder->read(next_query_info_while_reading, [this](bool ok)
            {
                /// Called on queue_thread.
                if (ok)
                {
                    const auto & nqi = next_query_info_while_reading;
                    if (check_query_info_contains_cancel_only)
                    {
                        if (!nqi.query().empty() || !nqi.query_id().empty() || !nqi.settings().empty() || !nqi.database().empty()
                            || !nqi.input_data().empty() || !nqi.input_data_delimiter().empty() || !nqi.output_format().empty()
                            || !nqi.user_name().empty() || !nqi.password().empty() || !nqi.quota().empty() || !nqi.session_id().empty())
                        {
                            LOG_WARNING(log, "Cannot add extra information to a query which is already executing. Only the 'cancel' field can be set");
                        }
                    }
                    if (nqi.cancel())
                        want_to_cancel = true;
                }
                else
                {
                    /// We cannot throw an exception right here because this code is executed
                    /// on queue_thread.
                    failed_to_read_query_info = true;
                }
                reading_query_info = false;
                read_finished.notify_one();
            });
        };

        auto finish_reading = [&]
        {
            if (reading_query_info)
            {
                Stopwatch client_writing_watch;
                std::unique_lock lock{dummy_mutex};
                read_finished.wait(lock, [this] { return !reading_query_info; });
                waited_for_client_writing += client_writing_watch.elapsedNanoseconds();
            }
            throwIfFailedToReadQueryInfo();
            query_info = std::move(next_query_info_while_reading);
            initial_query_info_read = true;
        };

        if (!initial_query_info_read)
        {
            /// Initial query info hasn't been read yet, so we're going to read it now.
            start_reading();
        }

        /// Maybe it's reading a query info right now. Let it finish.
        finish_reading();

        if (isInputStreaming(call_type))
        {
            /// Next query info can contain more input data. Now we start reading a next query info,
            /// so another call of readQueryInfo() in the future will probably be able to take it.
            start_reading();
        }
    }

    void Call::throwIfFailedToReadQueryInfo()
    {
        if (failed_to_read_query_info)
        {
            if (initial_query_info_read)
                throw Exception("Failed to read extra QueryInfo", ErrorCodes::NETWORK_ERROR);
            else
                throw Exception("Failed to read initial QueryInfo", ErrorCodes::NETWORK_ERROR);
        }
    }

    bool Call::isQueryCancelled()
    {
        if (cancelled)
        {
            result.set_cancelled(true);
            return true;
        }

        if (want_to_cancel)
        {
            LOG_INFO(log, "Query cancelled");
            cancelled = true;
            result.set_cancelled(true);
            return true;
        }

        return false;
    }

    void Call::addProgressToResult()
    {
        auto values = progress.fetchAndResetPiecewiseAtomically();
        if (!values.read_rows && !values.read_bytes && !values.total_rows_to_read && !values.written_rows && !values.written_bytes)
            return;
        auto & grpc_progress = *result.mutable_progress();
        /// Sum is used because we need to accumulate values for the case if streaming output is disabled.
        grpc_progress.set_read_rows(grpc_progress.read_rows() + values.read_rows);
        grpc_progress.set_read_bytes(grpc_progress.read_bytes() + values.read_bytes);
        grpc_progress.set_total_rows_to_read(grpc_progress.total_rows_to_read() + values.total_rows_to_read);
        grpc_progress.set_written_rows(grpc_progress.written_rows() + values.written_rows);
        grpc_progress.set_written_bytes(grpc_progress.written_bytes() + values.written_bytes);
    }

    void Call::addTotalsToResult(const Block & totals)
    {
        if (!totals)
            return;

        WriteBufferFromString buf{*result.mutable_totals()};
        auto stream = query_context->getOutputStream(output_format, buf, totals);
        stream->writePrefix();
        stream->write(totals);
        stream->writeSuffix();
    }

    void Call::addExtremesToResult(const Block & extremes)
    {
        if (!extremes)
            return;

        WriteBufferFromString buf{*result.mutable_extremes()};
        auto stream = query_context->getOutputStream(output_format, buf, extremes);
        stream->writePrefix();
        stream->write(extremes);
        stream->writeSuffix();
    }

    void Call::addProfileInfoToResult(const BlockStreamProfileInfo & info)
    {
        auto & stats = *result.mutable_stats();
        stats.set_rows(info.rows);
        stats.set_blocks(info.blocks);
        stats.set_allocated_bytes(info.bytes);
        stats.set_applied_limit(info.hasAppliedLimit());
        stats.set_rows_before_limit(info.getRowsBeforeLimit());
    }

    void Call::addLogsToResult()
    {
        if (!logs_queue)
            return;

        static_assert(::clickhouse::grpc::LOG_NONE        == 0);
        static_assert(::clickhouse::grpc::LOG_FATAL       == static_cast<int>(Poco::Message::PRIO_FATAL));
        static_assert(::clickhouse::grpc::LOG_CRITICAL    == static_cast<int>(Poco::Message::PRIO_CRITICAL));
        static_assert(::clickhouse::grpc::LOG_ERROR       == static_cast<int>(Poco::Message::PRIO_ERROR));
        static_assert(::clickhouse::grpc::LOG_WARNING     == static_cast<int>(Poco::Message::PRIO_WARNING));
        static_assert(::clickhouse::grpc::LOG_NOTICE      == static_cast<int>(Poco::Message::PRIO_NOTICE));
        static_assert(::clickhouse::grpc::LOG_INFORMATION == static_cast<int>(Poco::Message::PRIO_INFORMATION));
        static_assert(::clickhouse::grpc::LOG_DEBUG       == static_cast<int>(Poco::Message::PRIO_DEBUG));
        static_assert(::clickhouse::grpc::LOG_TRACE       == static_cast<int>(Poco::Message::PRIO_TRACE));

        MutableColumns columns;
        while (logs_queue->tryPop(columns))
        {
            if (columns.empty() || columns[0]->empty())
                continue;

            const auto & column_time = typeid_cast<const ColumnUInt32 &>(*columns[0]);
            const auto & column_time_microseconds = typeid_cast<const ColumnUInt32 &>(*columns[1]);
            const auto & column_query_id = typeid_cast<const ColumnString &>(*columns[3]);
            const auto & column_thread_id = typeid_cast<const ColumnUInt64 &>(*columns[4]);
            const auto & column_level = typeid_cast<const ColumnInt8 &>(*columns[5]);
            const auto & column_source = typeid_cast<const ColumnString &>(*columns[6]);
            const auto & column_text = typeid_cast<const ColumnString &>(*columns[7]);
            size_t num_rows = column_time.size();

            for (size_t row = 0; row != num_rows; ++row)
            {
                auto & log_entry = *result.add_logs();
                log_entry.set_time(column_time.getElement(row));
                log_entry.set_time_microseconds(column_time_microseconds.getElement(row));
                StringRef query_id = column_query_id.getDataAt(row);
                log_entry.set_query_id(query_id.data, query_id.size);
                log_entry.set_thread_id(column_thread_id.getElement(row));
                log_entry.set_level(static_cast<::clickhouse::grpc::LogsLevel>(column_level.getElement(row)));
                StringRef source = column_source.getDataAt(row);
                log_entry.set_source(source.data, source.size);
                StringRef text = column_text.getDataAt(row);
                log_entry.set_text(text.data, text.size);
            }
        }
    }

    void Call::sendResult()
    {
        /// gRPC doesn't allow to write anything to a finished responder.
        if (responder_finished)
            return;

        /// If output is not streaming then only the final result can be sent.
        bool send_final_message = finalize || result.has_exception() || result.cancelled();
        if (!send_final_message && !isOutputStreaming(call_type))
            return;

        /// Wait for previous write to finish.
        /// (gRPC doesn't allow to start sending another result while the previous is still being sending.)
        if (sending_result)
        {
            Stopwatch client_reading_watch;
            std::unique_lock lock{dummy_mutex};
            write_finished.wait(lock, [this] { return !sending_result; });
            waited_for_client_reading += client_reading_watch.elapsedNanoseconds();
        }
        throwIfFailedToSendResult();

        /// Start sending the result.
        LOG_DEBUG(log, "Sending {} result to the client: {}", (send_final_message ? "final" : "intermediate"), getResultDescription(result));

        if (write_buffer)
            write_buffer->finalize();

        sending_result = true;
        auto callback = [this](bool ok)
        {
            /// Called on queue_thread.
            if (!ok)
                failed_to_send_result = true;
            sending_result = false;
            write_finished.notify_one();
        };

        Stopwatch client_reading_final_watch;
        if (send_final_message)
        {
            responder_finished = true;
            responder->writeAndFinish(result, {}, callback);
        }
        else
            responder->write(result, callback);

        /// gRPC has already retrieved all data from `result`, so we don't have to keep it.
        result.Clear();
        if (write_buffer)
            write_buffer->restart();

        if (send_final_message)
        {
            /// Wait until the result is actually sent.
            std::unique_lock lock{dummy_mutex};
            write_finished.wait(lock, [this] { return !sending_result; });
            waited_for_client_reading += client_reading_final_watch.elapsedNanoseconds();
            throwIfFailedToSendResult();
            LOG_TRACE(log, "Final result has been sent to the client");
        }
    }

    void Call::throwIfFailedToSendResult()
    {
        if (failed_to_send_result)
            throw Exception("Failed to send result to the client", ErrorCodes::NETWORK_ERROR);
    }

    void Call::sendException(const Exception & exception)
    {
        auto & grpc_exception = *result.mutable_exception();
        grpc_exception.set_code(exception.code());
        grpc_exception.set_name(exception.name());
        grpc_exception.set_display_text(exception.displayText());
        if (send_exception_with_stacktrace)
            grpc_exception.set_stack_trace(exception.getStackTraceString());
        sendResult();
    }
}


class GRPCServer::Runner
{
public:
    explicit Runner(GRPCServer & owner_) : owner(owner_) {}

    ~Runner()
    {
        if (queue_thread.joinable())
            queue_thread.join();
    }

    void start()
    {
        startReceivingNewCalls();

        /// We run queue in a separate thread.
        auto runner_function = [this]
        {
            try
            {
                run();
            }
            catch (...)
            {
                tryLogCurrentException("GRPCServer");
            }
        };
        queue_thread = ThreadFromGlobalPool{runner_function};
    }

    void stop() { stopReceivingNewCalls(); }

    size_t getNumCurrentCalls() const
    {
        std::lock_guard lock{mutex};
        return current_calls.size();
    }

private:
    void startReceivingNewCalls()
    {
        std::lock_guard lock{mutex};
        responders_for_new_calls.resize(CALL_MAX);
        for (CallType call_type : ext::range(CALL_MAX))
            makeResponderForNewCall(call_type);
    }

    void makeResponderForNewCall(CallType call_type)
    {
        /// `mutex` is already locked.
        responders_for_new_calls[call_type] = makeResponder(call_type);

        responders_for_new_calls[call_type]->start(
            owner.grpc_service, *owner.queue, *owner.queue,
            [this, call_type](bool ok) { onNewCall(call_type, ok); });
    }

    void stopReceivingNewCalls()
    {
        std::lock_guard lock{mutex};
        should_stop = true;
    }

    void onNewCall(CallType call_type, bool responder_started_ok)
    {
        std::lock_guard lock{mutex};
        auto responder = std::move(responders_for_new_calls[call_type]);
        if (should_stop)
            return;
        makeResponderForNewCall(call_type);
        if (responder_started_ok)
        {
            /// Connection established and the responder has been started.
            /// So we pass this responder to a Call and make another responder for next connection.
            auto new_call = std::make_unique<Call>(call_type, std::move(responder), owner.iserver, owner.log);
            auto * new_call_ptr = new_call.get();
            current_calls[new_call_ptr] = std::move(new_call);
            new_call_ptr->start([this, new_call_ptr]() { onFinishCall(new_call_ptr); });
        }
    }

    void onFinishCall(Call * call)
    {
        /// Called on call_thread. That's why we can't destroy the `call` right now
        /// (thread can't join to itself). Thus here we only move the `call` from
        /// `current_call` to `finished_calls` and run() will actually destroy the `call`.
        std::lock_guard lock{mutex};
        auto it = current_calls.find(call);
        finished_calls.push_back(std::move(it->second));
        current_calls.erase(it);
    }

    void run()
    {
        while (true)
        {
            {
                std::lock_guard lock{mutex};
                finished_calls.clear(); /// Destroy finished calls.

                /// If (should_stop == true) we continue processing until there is no active calls.
                if (should_stop && current_calls.empty())
                {
                    bool all_responders_gone = std::all_of(
                        responders_for_new_calls.begin(), responders_for_new_calls.end(),
                        [](std::unique_ptr<BaseResponder> & responder) { return !responder; });
                    if (all_responders_gone)
                        break;
                }
            }

            bool ok = false;
            void * tag = nullptr;
            if (!owner.queue->Next(&tag, &ok))
            {
                /// Queue shutted down.
                break;
            }

            auto & callback = *static_cast<CompletionCallback *>(tag);
            callback(ok);
        }
    }

    GRPCServer & owner;
    ThreadFromGlobalPool queue_thread;
    std::vector<std::unique_ptr<BaseResponder>> responders_for_new_calls;
    std::map<Call *, std::unique_ptr<Call>> current_calls;
    std::vector<std::unique_ptr<Call>> finished_calls;
    bool should_stop = false;
    mutable std::mutex mutex;
};


GRPCServer::GRPCServer(IServer & iserver_, const Poco::Net::SocketAddress & address_to_listen_)
    : iserver(iserver_)
    , address_to_listen(address_to_listen_)
    , log(&Poco::Logger::get("GRPCServer"))
    , runner(std::make_unique<Runner>(*this))
{}

GRPCServer::~GRPCServer()
{
    /// Server should be shutdown before CompletionQueue.
    if (grpc_server)
        grpc_server->Shutdown();

    /// Completion Queue should be shutdown before destroying the runner,
    /// because the runner is now probably executing CompletionQueue::Next() on queue_thread
    /// which is blocked until an event is available or the queue is shutting down.
    if (queue)
        queue->Shutdown();

    runner.reset();
}

void GRPCServer::start()
{
    initGRPCLogging(iserver.config());
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address_to_listen.toString(), makeCredentials(iserver.config()));
    builder.RegisterService(&grpc_service);
    builder.SetMaxSendMessageSize(iserver.config().getInt("grpc.max_send_message_size", -1));
    builder.SetMaxReceiveMessageSize(iserver.config().getInt("grpc.max_receive_message_size", -1));
    builder.SetDefaultCompressionAlgorithm(parseCompressionAlgorithm(iserver.config().getString("grpc.compression", "none")));
    builder.SetDefaultCompressionLevel(parseCompressionLevel(iserver.config().getString("grpc.compression_level", "none")));

    queue = builder.AddCompletionQueue();
    grpc_server = builder.BuildAndStart();
    runner->start();
}


void GRPCServer::stop()
{
    /// Stop receiving new calls.
    runner->stop();
}

size_t GRPCServer::currentConnections() const
{
    return runner->getNumCurrentCalls();
}

}
#endif
