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
#include <Server/WriteBufferFromGRPC.h>
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
    /// Requests a connection and provides low-level interface for reading and writing.
    class Responder
    {
    public:
        Responder() : tag(this) {}

        void setTag(void * tag_) { tag = tag_; }

        void start(GRPCService & grpc_service, grpc::ServerCompletionQueue & new_call_queue, grpc::ServerCompletionQueue & notification_queue)
        {
            grpc_service.RequestExecuteQuery(&grpc_context, &reader_writer, &new_call_queue, &notification_queue, tag);
        }

        void read(GRPCQueryInfo & query_info_)
        {
            reader_writer.Read(&query_info_, tag);
        }

        /*void write(const GRPCResult & result)
        {
            reader_writer.Write(result, tag);
        }*/

        void writeAndFinish(const GRPCResult & result, const grpc::Status & status)
        {
            reader_writer.WriteAndFinish(result, {}, status, tag);
        }

        Poco::Net::SocketAddress getClientAddress() const
        {
            String peer = grpc_context.peer();
            return Poco::Net::SocketAddress{peer.substr(peer.find(':') + 1)};
        }

        grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryInfo> & getReaderWriter()
        {
            return reader_writer;
        }

    private:
        grpc::ServerContext grpc_context;
        grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryInfo> reader_writer{&grpc_context};
        void * tag;
    };


    /// Handles a connection after a responder is started (i.e. after getting a new call).
    class Call
    {
    public:
        Call(std::unique_ptr<Responder> responder_, IServer & iserver_, Poco::Logger * log_);
        ~Call();

        void start(const std::function<void(void)> & on_finish_call_callback);
        void sync(bool ok);

    private:
        void run();
        void waitForSync();

        void receiveQuery();
        void executeQuery();
        void processInput();
        void generateOutput();
        void generateOutputWithProcessors();
        void finishQuery();
        void onException(const Exception & exception);
        void close();

        void sendOutput(const Block & block);
        void sendProgress();
        void sendTotals(const Block & totals);
        void sendExtremes(const Block & extremes);
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
        std::shared_ptr<WriteBufferFromGRPC> out;
        Progress progress;

        GRPCQueryInfo query_info; /// We reuse the same messages multiple times.
        GRPCResult result;

        ThreadFromGlobalPool call_thread;
        std::condition_variable signal;
        std::atomic<size_t> num_syncs_pending = 0;
        std::atomic<bool> sync_failed = false;
    };

    Call::Call(std::unique_ptr<Responder> responder_, IServer & iserver_, Poco::Logger * log_)
        : responder(std::move(responder_)), iserver(iserver_), log(log_)
    {
        responder->setTag(this);
        out = std::make_shared<WriteBufferFromGRPC>(&responder->getReaderWriter(), this, nullptr);
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

    void Call::sync(bool ok)
    {
        ++num_syncs_pending;
        if (!ok)
            sync_failed = true;
        signal.notify_one();
    }

    void Call::waitForSync()
    {
        std::mutex mutex;
        std::unique_lock lock{mutex};
        signal.wait(lock, [&] { return (num_syncs_pending > 0) || sync_failed; });
        if (sync_failed)
            throw Exception("Client has gone away or network failure", ErrorCodes::NETWORK_ERROR);
        --num_syncs_pending;
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
        responder->read(query_info);
        waitForSync();
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
            responder->read(query_info);
            waitForSync();
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
            if (async_in.poll(interactive_delay / 1000))
            {
                const auto block = async_in.read();
                if (!block)
                    break;

                if (!io.null_format)
                    sendOutput(block);
            }

            if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
            {
                sendProgress();
                after_send_progress.restart();
            }
        }
        async_in.readSuffix();

        sendTotals(io.in->getTotals());
        sendExtremes(io.in->getExtremes());
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
            if (block)
            {
                if (!io.null_format)
                    sendOutput(block);
            }

            if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
            {
                sendProgress();
                after_send_progress.restart();
            }
        }

        sendTotals(executor->getTotalsBlock());
        sendExtremes(executor->getExtremesBlock());
    }

    void Call::finishQuery()
    {
        io.onFinish();
        query_scope->logPeakMemoryUsage();
        out->finalize();
        waitForSync();
    }

    void Call::onException(const Exception & exception)
    {
        io.onException();

        if (responder)
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
        out.reset();
        query_scope.reset();
        query_context.reset();
    }

    void Call::sendOutput(const Block & block)
    {
        out->setResponse([](const String & buffer)
        {
            GRPCResult tmp_response;
            tmp_response.set_output(buffer);
            return tmp_response;
        });
        auto my_block_out_stream = query_context->getOutputFormat(output_format, *out, block);
        my_block_out_stream->write(block);
        my_block_out_stream->flush();
        out->next();
        waitForSync();
    }

    void Call::sendProgress()
    {
        auto grpc_progress = [](const String & buffer)
        {
            auto in = std::make_unique<ReadBufferFromString>(buffer);
            ProgressValues progress_values;
            progress_values.read(*in, DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO);
            GRPCProgress tmp_progress;
            tmp_progress.set_read_rows(progress_values.read_rows);
            tmp_progress.set_read_bytes(progress_values.read_bytes);
            tmp_progress.set_total_rows_to_read(progress_values.total_rows_to_read);
            tmp_progress.set_written_rows(progress_values.written_rows);
            tmp_progress.set_written_bytes(progress_values.written_bytes);
            return tmp_progress;
        };

        out->setResponse([&grpc_progress](const String & buffer)
        {
            GRPCResult tmp_response;
            auto tmp_progress = std::make_unique<GRPCProgress>(grpc_progress(buffer));
            tmp_response.set_allocated_progress(tmp_progress.release());
            return tmp_response;
        });
        auto increment = progress.fetchAndResetPiecewiseAtomically();
        increment.write(*out, DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO);
        out->next();
        waitForSync();
    }

    void Call::sendTotals(const Block & totals)
    {
        if (totals)
        {
            out->setResponse([](const String & buffer)
            {
                GRPCResult tmp_response;
                tmp_response.set_totals(buffer);
                return tmp_response;
            });
            auto my_block_out_stream = query_context->getOutputFormat(output_format, *out, totals);
            my_block_out_stream->write(totals);
            my_block_out_stream->flush();
            out->next();
            waitForSync();
        }
    }

    void Call::sendExtremes(const Block & extremes)
    {
        if (extremes)
        {
            out->setResponse([](const String & buffer)
            {
                GRPCResult tmp_response;
                tmp_response.set_extremes(buffer);
                return tmp_response;
            });
            auto my_block_out_stream = query_context->getOutputFormat(output_format, *out, extremes);
            my_block_out_stream->write(extremes);
            my_block_out_stream->flush();
            out->next();
            waitForSync();
        }
    }

    void Call::sendException(const Exception & exception)
    {
        auto & grpc_exception = *result.mutable_exception();
        grpc_exception.set_code(exception.code());
        grpc_exception.set_message(getExceptionMessage(exception, send_exception_with_stacktrace, true));
        responder->writeAndFinish(result, {});
        waitForSync();
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
        responder_for_new_call->start(owner.grpc_service, *owner.queue, *owner.queue);
    }

    void stopReceivingNewCalls()
    {
        std::lock_guard lock{mutex};
        should_stop = true;
    }

    void onNewCall(bool responder_started)
    {
        /// `mutex` is already locked.
        auto responder = std::move(responder_for_new_call);
        if (should_stop)
            return;
        makeResponderForNewCall();
        if (responder_started)
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

            {
                std::lock_guard lock{mutex};
                if (tag == responder_for_new_call.get())
                {
                    onNewCall(ok);
                    continue;
                }
            }

            /// Continue handling a Call.
            auto call = static_cast<Call *>(tag);
            call->sync(ok);
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
