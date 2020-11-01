#include "GRPCServer.h"
#if USE_GRPC

#include <Common/CurrentThread.h>
#include <Common/SettingsChanges.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Server/IServer.h>
#include <Storages/IStorage.h>
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
    extern const int NETWORK_ERROR;
    extern const int NO_DATA_TO_INSERT;
    extern const int UNKNOWN_DATABASE;
}

namespace
{
    using CompletionCallback = std::function<void(bool)>;

    /// Requests a connection and provides low-level interface for reading and writing.
    class Responder
    {
    public:
        void start(
            GRPCService & grpc_service,
            grpc::ServerCompletionQueue & new_call_queue,
            grpc::ServerCompletionQueue & notification_queue,
            const CompletionCallback & callback)
        {
            grpc_service.RequestExecuteQuery(&grpc_context, &reader_writer, &new_call_queue, &notification_queue, getCallbackPtr(callback));
        }

        void read(GRPCQueryInfo & query_info_, const CompletionCallback & callback)
        {
            reader_writer.Read(&query_info_, getCallbackPtr(callback));
        }

        void write(const GRPCResult & result, const CompletionCallback & callback)
        {
            reader_writer.Write(result, getCallbackPtr(callback));
        }

        void writeAndFinish(const GRPCResult & result, const grpc::Status & status, const CompletionCallback & callback)
        {
            reader_writer.WriteAndFinish(result, {}, status, getCallbackPtr(callback));
        }

        Poco::Net::SocketAddress getClientAddress() const
        {
            String peer = grpc_context.peer();
            return Poco::Net::SocketAddress{peer.substr(peer.find(':') + 1)};
        }

    private:
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
        grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryInfo> reader_writer{&grpc_context};
        std::unordered_map<size_t, CompletionCallback> callbacks;
        size_t next_callback_id = 0;
        std::mutex mutex;
    };


    /// Handles a connection after a responder is started (i.e. after getting a new call).
    class Call
    {
    public:
        Call(std::unique_ptr<Responder> responder_, IServer & iserver_, Poco::Logger * log_);
        ~Call();

        void start(const std::function<void(void)> & on_finish_call_callback);

    private:
        void run();

        void receiveQuery();
        void executeQuery();
        void processInput();
        void generateOutput();
        void generateOutputWithProcessors();
        void finishQuery();
        void onException(const Exception & exception);
        void close();

        void readQueryInfo();
        void addOutputToResult(const Block & block);
        void addProgressToResult();
        void addTotalsToResult(const Block & totals);
        void addExtremesToResult(const Block & extremes);
        void sendResult();
        void throwIfFailedToSendResult();
        void sendException(const Exception & exception);

        std::unique_ptr<Responder> responder;
        IServer & iserver;
        Poco::Logger * log = nullptr;

        std::optional<Context> query_context;
        std::optional<CurrentThread::QueryScope> query_scope;
        String query_text;
        ASTPtr ast;
        ASTInsertQuery * insert_query = nullptr;
        String input_format;
        String output_format;
        uint64_t interactive_delay = 100000;
        bool send_exception_with_stacktrace = true;

        BlockIO io;
        Progress progress;

        GRPCQueryInfo query_info; /// We reuse the same messages multiple times.
        GRPCResult result;

        /// 0 - no query info has been read, 1 - initial query info, 2 - next query info, ...
        size_t query_info_index = 0;

        bool finalize = false;
        bool responder_finished = false;

        std::atomic<bool> sending_result = false; /// atomic because it can be accessed both from call_thread and queue_thread
        std::atomic<bool> failed_to_send_result = false;

        ThreadFromGlobalPool call_thread;
        std::condition_variable read_finished;
        std::condition_variable write_finished;
        std::mutex dummy_mutex; /// Doesn't protect anything.
    };

    Call::Call(std::unique_ptr<Responder> responder_, IServer & iserver_, Poco::Logger * log_)
        : responder(std::move(responder_)), iserver(iserver_), log(log_)
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
        readQueryInfo();
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
        query_context.emplace(iserver.context());
        query_scope.emplace(*query_context);

        /// Authentication.
        query_context->setUser(user, password, user_address);
        query_context->setCurrentQueryId(query_info.query_id());
        if (!quota_key.empty())
            query_context->setQuotaKey(quota_key);

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
        query_text = std::move(*query_info.mutable_query());
        const char * begin = query_text.data();
        const char * end = begin + query_text.size();
        ParserQuery parser(end);
        ast = parseQuery(parser, begin, end, "", settings.max_query_size, settings.max_parser_depth);

        /// Choose output format.
        output_format = "Values";
        if (!query_info.output_format().empty())
        {
            output_format = query_info.output_format();
            query_context->setDefaultFormat(query_info.output_format());
        }

        /// Start executing the query.
        insert_query = ast->as<ASTInsertQuery>();
        const auto * query_end = end;
        if (insert_query && insert_query->data)
        {
            query_end = insert_query->data;
        }
        String query(begin, query_end);
        io = ::DB::executeQuery(query, *query_context, false, QueryProcessingStage::Complete, true, true);
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

        /// Choose input format.
        if (insert_query)
        {
            input_format = insert_query->format;
            if (input_format.empty())
                input_format = "Values";
        }

        if (output_format.empty())
            output_format = input_format;

        /// Prepare read buffer with data to insert.
        ConcatReadBuffer::ReadBuffers buffers;
        std::shared_ptr<ReadBufferFromMemory> insert_query_data_buffer;
        std::shared_ptr<ReadBufferFromMemory> input_data_buffer;
        if (insert_query && insert_query->data)
        {
            insert_query_data_buffer = std::make_shared<ReadBufferFromMemory>(insert_query->data, insert_query->end - insert_query->data);
            buffers.push_back(insert_query_data_buffer.get());
        }
        if (!query_info.input_data().empty())
        {
            input_data_buffer = std::make_shared<ReadBufferFromMemory>(query_info.input_data().data(), query_info.input_data().size());
            buffers.push_back(input_data_buffer.get());
        }
        auto input_buffer_contacenated = std::make_unique<ConcatReadBuffer>(buffers);
        auto res_stream = query_context->getInputFormat(
            input_format, *input_buffer_contacenated, io.out->getHeader(), query_context->getSettings().max_insert_block_size);

        /// Add default values if necessary.
        if (insert_query)
        {
            auto table_id = query_context->resolveStorageID(insert_query->table_id, Context::ResolveOrdinary);
            if (query_context->getSettingsRef().input_format_defaults_for_omitted_fields && table_id)
            {
                StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, *query_context);
                const auto & columns = storage->getInMemoryMetadataPtr()->getColumns();
                if (!columns.empty())
                    res_stream = std::make_shared<AddingDefaultsBlockInputStream>(res_stream, columns, *query_context);
            }
        }

        /// Read input data.
        io.out->writePrefix();

        while (auto block = res_stream->read())
            io.out->write(block);

        while (query_info.next_query_info())
        {
            readQueryInfo();
            if (!query_info.input_data().empty())
            {
                const char * begin = query_info.input_data().data();
                const char * end = begin + query_info.input_data().size();
                ReadBufferFromMemory data_in(begin, end - begin);
                res_stream = query_context->getInputFormat(
                    input_format, data_in, io.out->getHeader(), query_context->getSettings().max_insert_block_size);

                while (auto block = res_stream->read())
                    io.out->write(block);
            }
        }

        io.out->writeSuffix();
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
        Stopwatch after_send_progress;

        async_in.readPrefix();
        while (true)
        {
            Block block;
            if (async_in.poll(interactive_delay / 1000))
            {
                block = async_in.read();
                if (!block)
                    break;
            }

            throwIfFailedToSendResult();

            if (block && !io.null_format)
                addOutputToResult(block);

            if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
            {
                addProgressToResult();
                after_send_progress.restart();
            }

            if (!result.output().empty() || result.has_progress())
                sendResult();

            throwIfFailedToSendResult();
        }
        async_in.readSuffix();

        addTotalsToResult(io.in->getTotals());
        addExtremesToResult(io.in->getExtremes());
    }

    void Call::generateOutputWithProcessors()
    {
        if (!io.pipeline.initialized())
            return;

        auto executor = std::make_shared<PullingAsyncPipelineExecutor>(io.pipeline);
        Stopwatch after_send_progress;

        Block block;
        while (executor->pull(block, interactive_delay / 1000))
        {
            throwIfFailedToSendResult();

            if (block && !io.null_format)
                addOutputToResult(block);

            if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
            {
                addProgressToResult();
                after_send_progress.restart();
            }

            if (!result.output().empty() || result.has_progress())
                sendResult();

            throwIfFailedToSendResult();
        }

        addTotalsToResult(executor->getTotalsBlock());
        addExtremesToResult(executor->getExtremesBlock());
    }

    void Call::finishQuery()
    {
        finalize = true;
        io.onFinish();
        query_scope->logPeakMemoryUsage();
        sendResult();
        close();
    }

    void Call::onException(const Exception & exception)
    {
        io.onException();

        if (responder && !responder_finished)
        {
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

    void Call::close()
    {
        responder.reset();
        io = {};
        query_scope.reset();
        query_context.reset();
    }

    void Call::readQueryInfo()
    {
        bool ok = false;

        /// Start reading a query info.
        bool reading_query_info = true;
        responder->read(query_info, [&](bool ok_)
        {
            /// Called on queue_thread.
            ok = ok_;
            reading_query_info = false;
            read_finished.notify_one();
        });

        /// Wait until the reading is finished.
        std::unique_lock lock{dummy_mutex};
        read_finished.wait(lock, [&] { return !reading_query_info; });

        if (!ok)
        {
            if (!query_info_index)
                throw Exception("Failed to read initial QueryInfo", ErrorCodes::NETWORK_ERROR);
            else
                throw Exception("Failed to read extra QueryInfo with input data", ErrorCodes::NETWORK_ERROR);
        }

        ++query_info_index;
    }

    void Call::addOutputToResult(const Block & block)
    {
        WriteBufferFromString buf{*result.mutable_output()};
        auto stream = query_context->getOutputFormat(output_format, buf, block);
        stream->write(block);
    }

    void Call::addProgressToResult()
    {
        auto & grpc_progress = *result.mutable_progress();
        auto values = progress.fetchAndResetPiecewiseAtomically();
        grpc_progress.set_read_rows(values.read_rows);
        grpc_progress.set_read_bytes(values.read_bytes);
        grpc_progress.set_total_rows_to_read(values.total_rows_to_read);
        grpc_progress.set_written_rows(values.written_rows);
        grpc_progress.set_written_bytes(values.written_bytes);
    }

    void Call::addTotalsToResult(const Block & totals)
    {
        if (!totals)
            return;

        WriteBufferFromString buf{*result.mutable_totals()};
        auto stream = query_context->getOutputFormat(output_format, buf, totals);
        stream->write(totals);
    }

    void Call::addExtremesToResult(const Block & extremes)
    {
        if (!extremes)
            return;

        WriteBufferFromString buf{*result.mutable_extremes()};
        auto stream = query_context->getOutputFormat(output_format, buf, extremes);
        stream->write(extremes);
    }

    void Call::sendResult()
    {
        /// gRPC doesn't allow to write anything to a finished responder.
        if (responder_finished)
            return;

        /// Wait for previous write to finish.
        /// (gRPC doesn't allow to start sending another result while the previous is still being sending.)
        if (sending_result)
        {
            std::unique_lock lock{dummy_mutex};
            write_finished.wait(lock, [this] { return !sending_result; });
        }
        throwIfFailedToSendResult();

        /// Start sending the result.
        sending_result = true;
        auto callback = [this](bool ok)
        {
            /// Called on queue_thread.
            if (!ok)
                failed_to_send_result = true;
            sending_result = false;
            write_finished.notify_one();
        };

        bool send_final_message = finalize || result.has_exception();
        if (send_final_message)
        {
            responder_finished = true;
            responder->writeAndFinish(result, {}, callback);
        }
        else
            responder->write(result, callback);

        /// gRPC has already retrieved all data from `result`, so we don't have to keep it.
        result.Clear();

        if (send_final_message)
        {
            /// Wait until the result is actually sent.
            std::unique_lock lock{dummy_mutex};
            write_finished.wait(lock, [this] { return !sending_result; });
            throwIfFailedToSendResult();
        }
    }

    void Call::throwIfFailedToSendResult()
    {
        if (failed_to_send_result)
            throw Exception("Failed to send result to client", ErrorCodes::NETWORK_ERROR);
    }

    void Call::sendException(const Exception & exception)
    {
        auto & grpc_exception = *result.mutable_exception();
        grpc_exception.set_code(exception.code());
        grpc_exception.set_message(getExceptionMessage(exception, send_exception_with_stacktrace, true));
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
        makeResponderForNewCall();
    }

    void makeResponderForNewCall()
    {
        /// `mutex` is already locked.
        responder_for_new_call = std::make_unique<Responder>();
        responder_for_new_call->start(owner.grpc_service, *owner.queue, *owner.queue, [this](bool ok) { onNewCall(ok); });
    }

    void stopReceivingNewCalls()
    {
        std::lock_guard lock{mutex};
        should_stop = true;
    }

    void onNewCall(bool responder_started_ok)
    {
        std::lock_guard lock{mutex};
        auto responder = std::move(responder_for_new_call);
        if (should_stop)
            return;
        makeResponderForNewCall();
        if (responder_started_ok)
        {
            /// Connection established and the responder has been started.
            /// So we pass this responder to a Call and make another responder for next connection.
            auto new_call = std::make_unique<Call>(std::move(responder), owner.iserver, owner.log);
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
                if (should_stop && current_calls.empty() && !responder_for_new_call)
                    break;
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
    std::unique_ptr<Responder> responder_for_new_call;
    std::map<Call *, std::unique_ptr<Call>> current_calls;
    std::vector<std::unique_ptr<Call>> finished_calls;
    bool should_stop = false;
    mutable std::mutex mutex;
};


GRPCServer::GRPCServer(IServer & iserver_, const Poco::Net::SocketAddress & address_to_listen_)
    : iserver(iserver_), address_to_listen(address_to_listen_), log(&Poco::Logger::get("GRPCServer"))
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
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address_to_listen.toString(), grpc::InsecureServerCredentials());
    builder.RegisterService(&grpc_service);
    builder.SetMaxReceiveMessageSize(INT_MAX);

    queue = builder.AddCompletionQueue();
    grpc_server = builder.BuildAndStart();
    runner = std::make_unique<Runner>(*this);
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
