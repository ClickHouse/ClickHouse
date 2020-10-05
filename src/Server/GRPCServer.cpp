#include "GRPCServer.h"
#if USE_GRPC

#include <Common/CurrentThread.h>
#include <Common/SettingsChanges.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
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
    extern const int UNKNOWN_DATABASE;
    extern const int NO_DATA_TO_INSERT;
}


namespace
{
    class CommonCallData
    {
    public:
        GRPCService * grpc_service;
        grpc::ServerCompletionQueue * notification_cq;
        grpc::ServerCompletionQueue * new_call_cq;
        grpc::ServerContext grpc_context;
        IServer * iserver;
        bool with_stacktrace = false;
        Poco::Logger * log;
        std::unique_ptr<CommonCallData> next_client;

        explicit CommonCallData(
            GRPCService * grpc_service_,
            grpc::ServerCompletionQueue * notification_cq_,
            grpc::ServerCompletionQueue * new_call_cq_,
            IServer * iserver_,
            Poco::Logger * log_)
            : grpc_service(grpc_service_), notification_cq(notification_cq_), new_call_cq(new_call_cq_), iserver(iserver_), log(log_)
        {
        }
        virtual ~CommonCallData() = default;
        virtual void respond() = 0;
    };

    class CallDataQuery : public CommonCallData
    {
    public:
        CallDataQuery(
            GRPCService * grpc_service_,
            grpc::ServerCompletionQueue * notification_cq_,
            grpc::ServerCompletionQueue * new_call_cq_,
            IServer * iserver_,
            Poco::Logger * log_)
            : CommonCallData(grpc_service_, notification_cq_, new_call_cq_, iserver_, log_), responder(&grpc_context), context(iserver->context())
        {
            details_status = SEND_TOTALS;
            status = START_QUERY;
            out = std::make_shared<WriteBufferFromGRPC>(&responder, static_cast<void *>(this), nullptr);
            grpc_service->RequestExecuteQuery(&grpc_context, &responder, new_call_cq, notification_cq, this);
        }
        void parseQuery();
        void parseData();
        void readData();
        void executeQuery();
        void progressQuery();
        void finishQuery();

        enum DetailsStatus
        {
            SEND_TOTALS,
            SEND_EXTREMES,
            //SEND_PROFILEINFO, //?
            FINISH
        };
        void sendDetails();

        bool sendData(const Block & block);
        bool sendProgress();
        bool sendTotals(const Block & totals);
        bool sendExtremes(const Block & extremes);

        enum Status
        {
            START_QUERY,
            PARSE_QUERY,
            READ_DATA,
            PROGRESS,
            FINISH_QUERY
        };
        virtual void respond() override;
        virtual ~CallDataQuery() override
        {
            query_watch.stop();
            progress_watch.stop();
            query_context.reset();
            query_scope.reset();
        }

    private:
        GRPCQueryInfo request;
        GRPCResult response;
        grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryInfo> responder;

        Stopwatch progress_watch;
        Stopwatch query_watch;
        Progress progress;

        DetailsStatus details_status;
        Status status;

        BlockIO io;
        Context context;
        std::shared_ptr<PullingAsyncPipelineExecutor> executor;
        std::optional<Context> query_context;

        std::shared_ptr<WriteBufferFromGRPC> out;
        String format_output;
        String format_input;
        uint64_t interactive_delay;
        std::optional<CurrentThread::QueryScope> query_scope;
    };

    std::string parseGRPCPeer(const grpc::ServerContext & context_)
    {
        String info = context_.peer();
        return info.substr(info.find(':') + 1);
    }

    void CallDataQuery::respond()
    {
        try
        {
            switch (status)
            {
                case START_QUERY: {
                    new CallDataQuery(grpc_service, notification_cq, new_call_cq, iserver, log);
                    status = PARSE_QUERY;
                    responder.Read(&request, static_cast<void *>(this));
                    break;
                }
                case PARSE_QUERY: {
                    parseQuery();
                    parseData();
                    break;
                }
                case READ_DATA: {
                    readData();
                    break;
                }
                case PROGRESS: {
                    progressQuery();
                    break;
                }
                case FINISH_QUERY: {
                    delete this;
                }
            }
        }
        catch (...)
        {
            io.onException();

            tryLogCurrentException(log);
            auto & grpc_exception = *response.mutable_exception();
            grpc_exception.set_code(getCurrentExceptionCode());
            grpc_exception.set_message(getCurrentExceptionMessage(with_stacktrace, true));
            status = FINISH_QUERY;
            responder.WriteAndFinish(response, grpc::WriteOptions(), grpc::Status(), static_cast<void *>(this));
        }
    }

    void CallDataQuery::parseQuery()
    {
        LOG_TRACE(log, "Process query");

        Poco::Net::SocketAddress user_adress(parseGRPCPeer(grpc_context));
        LOG_TRACE(log, "Request: {}", request.query());

        std::string user = request.user_name();
        std::string password = request.password();
        std::string quota_key = request.quota();
        format_output = "Values";
        if (user.empty())
        {
            user = "default";
            password = "";
        }
        context.setProgressCallback([this](const Progress & value) { return progress.incrementPiecewiseAtomically(value); });


        query_context = context;
        query_scope.emplace(*query_context);
        query_context->setUser(user, password, user_adress);
        query_context->setCurrentQueryId(request.query_id());
        if (!quota_key.empty())
            query_context->setQuotaKey(quota_key);

        if (!request.output_format().empty())
        {
            format_output = request.output_format();
            query_context->setDefaultFormat(request.output_format());
        }
        if (!request.database().empty())
        {
            if (!DatabaseCatalog::instance().isDatabaseExist(request.database()))
            {
                Exception e("Database " + request.database() + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
            }
            query_context->setCurrentDatabase(request.database());
        }

        SettingsChanges settings_changes;
        for (const auto & [key, value] : request.settings())
        {
            settings_changes.push_back({key, value});
        }
        query_context->checkSettingsConstraints(settings_changes);
        query_context->applySettingsChanges(settings_changes);

        interactive_delay = query_context->getSettingsRef().interactive_delay;

        ClientInfo & client_info = query_context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;

        client_info.initial_user = client_info.current_user;
        client_info.initial_query_id = client_info.current_query_id;
        client_info.initial_address = client_info.current_address;
    }

    void CallDataQuery::parseData()
    {
        LOG_TRACE(log, "ParseData");
        const char * begin = request.query().data();
        const char * end = begin + request.query().size();
        const Settings & settings = query_context->getSettingsRef();

        ParserQuery parser(end);
        ASTPtr ast = ::DB::parseQuery(parser, begin, end, "", settings.max_query_size, settings.max_parser_depth);

        auto * insert_query = ast->as<ASTInsertQuery>();
        const auto * query_end = end;

        if (insert_query && insert_query->data)
        {
            query_end = insert_query->data;
        }
        String query(begin, query_end);
        io = ::DB::executeQuery(query, *query_context, false, QueryProcessingStage::Complete, true, true);
        if (io.out)
        {
            if (!insert_query || !(insert_query->data || !request.input_data().empty() || request.next_query_info()))
            {
                Exception e("Logical error: query requires data to insert, but it is not INSERT query", ErrorCodes::NO_DATA_TO_INSERT);
            }

            format_input = insert_query->format;
            if (format_input.empty())
                format_input = "Values";

            if (format_output.empty())
                format_output = format_input;
            ConcatReadBuffer::ReadBuffers buffers;
            std::shared_ptr<ReadBufferFromMemory> data_in_query;
            std::shared_ptr<ReadBufferFromMemory> data_in_insert_data;
            if (insert_query->data)
            {
                data_in_query = std::make_shared<ReadBufferFromMemory>(insert_query->data, insert_query->end - insert_query->data);
                buffers.push_back(data_in_query.get());
            }

            if (!request.input_data().empty())
            {
                data_in_insert_data = std::make_shared<ReadBufferFromMemory>(request.input_data().data(), request.input_data().size());
                buffers.push_back(data_in_insert_data.get());
            }
            auto input_buffer_contacenated = std::make_unique<ConcatReadBuffer>(buffers);
            auto res_stream = query_context->getInputFormat(
                format_input, *input_buffer_contacenated, io.out->getHeader(), query_context->getSettings().max_insert_block_size);

            auto table_id = query_context->resolveStorageID(insert_query->table_id, Context::ResolveOrdinary);
            if (query_context->getSettingsRef().input_format_defaults_for_omitted_fields && table_id)
            {
                StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, *query_context);
                const auto & columns = storage->getInMemoryMetadataPtr()->getColumns();
                if (!columns.empty())
                    res_stream = std::make_shared<AddingDefaultsBlockInputStream>(res_stream, columns, *query_context);
            }
            io.out->writePrefix();
            while (auto block = res_stream->read())
                io.out->write(block);
            if (request.next_query_info())
            {
                status = READ_DATA;
                responder.Read(&request, static_cast<void *>(this));
                return;
            }
            io.out->writeSuffix();
        }

        executeQuery();
    }

    void CallDataQuery::readData()
    {
        if (!request.input_data().empty())
        {
            const char * begin = request.input_data().data();
            const char * end = begin + request.input_data().size();
            ReadBufferFromMemory data_in(begin, end - begin);
            auto res_stream = query_context->getInputFormat(
                format_input, data_in, io.out->getHeader(), query_context->getSettings().max_insert_block_size);

            while (auto block = res_stream->read())
                io.out->write(block);
        }

        if (request.next_query_info())
        {
            responder.Read(&request, static_cast<void *>(this));
        }
        else
        {
            io.out->writeSuffix();
            executeQuery();
        }
    }

    void CallDataQuery::executeQuery()
    {
        LOG_TRACE(log, "Execute Query");
        if (io.pipeline.initialized())
        {
            query_watch.start();
            progress_watch.start();
            executor = std::make_shared<PullingAsyncPipelineExecutor>(io.pipeline);
            progressQuery();
        }
        else
        {
            finishQuery();
        }
    }

    void CallDataQuery::progressQuery()
    {
        status = PROGRESS;
        bool sent = false;

        Block block;
        while (executor->pull(block, query_watch.elapsedMilliseconds()))
        {
            if (block)
            {
                if (!io.null_format)
                {
                    sent = sendData(block);
                    break; //?
                }
            }
            if (progress_watch.elapsedMilliseconds() >= interactive_delay)
            {
                progress_watch.restart();
                sent = sendProgress();
                break;
            }
            query_watch.restart();
        }
        if (!sent)
        {
            sendDetails();
        }
    }

    void CallDataQuery::sendDetails()
    {
        bool sent = false;
        while (!sent)
        {
            switch (details_status)
            {
                case SEND_TOTALS: {
                    sent = sendTotals(executor->getTotalsBlock());
                    details_status = SEND_EXTREMES;
                    break;
                }
                case SEND_EXTREMES: {
                    sent = sendExtremes(executor->getExtremesBlock());
                    details_status = FINISH;
                    break;
                }
                case FINISH: {
                    sent = true;
                    finishQuery();
                    break;
                }
            }
        }
    }

    void CallDataQuery::finishQuery()
    {
        io.onFinish();
        query_scope->logPeakMemoryUsage();
        status = FINISH_QUERY;
        out->finalize();
    }

    bool CallDataQuery::sendData(const Block & block)
    {
        out->setResponse([](const String & buffer)
        {
            GRPCResult tmp_response;
            tmp_response.set_output(buffer);
            return tmp_response;
        });
        auto my_block_out_stream = query_context->getOutputFormat(format_output, *out, block);
        my_block_out_stream->write(block);
        my_block_out_stream->flush();
        out->next();
        return true;
    }

    bool CallDataQuery::sendProgress()
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
        return true;
    }

    bool CallDataQuery::sendTotals(const Block & totals)
    {
        if (totals)
        {
            out->setResponse([](const String & buffer)
            {
                GRPCResult tmp_response;
                tmp_response.set_totals(buffer);
                return tmp_response;
            });
            auto my_block_out_stream = query_context->getOutputFormat(format_output, *out, totals);
            my_block_out_stream->write(totals);
            my_block_out_stream->flush();
            out->next();
            return true;
        }
        return false;
    }

    bool CallDataQuery::sendExtremes(const Block & extremes)
    {
        if (extremes)
        {
            out->setResponse([](const String & buffer)
            {
                GRPCResult tmp_response;
                tmp_response.set_extremes(buffer);
                return tmp_response;
            });
            auto my_block_out_stream = query_context->getOutputFormat(format_output, *out, extremes);
            my_block_out_stream->write(extremes);
            my_block_out_stream->flush();
            out->next();
            return true;
        }
        return false;
    }
}


GRPCServer::GRPCServer(std::string address_to_listen_, IServer & server_) : iserver(server_), log(&Poco::Logger::get("GRPCHandler"))
{
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address_to_listen_, grpc::InsecureServerCredentials());
    //keepalive pings default values
    builder.RegisterService(&grpc_service);
    builder.SetMaxReceiveMessageSize(INT_MAX);
    notification_cq = builder.AddCompletionQueue();
    new_call_cq = builder.AddCompletionQueue();
    grpc_server = builder.BuildAndStart();
}

GRPCServer::~GRPCServer() = default;

void GRPCServer::stop()
{
    grpc_server->Shutdown();
    notification_cq->Shutdown();
    new_call_cq->Shutdown();
}

void GRPCServer::run()
{
    HandleRpcs();
}

void GRPCServer::HandleRpcs()
{
    new CallDataQuery(&grpc_service, notification_cq.get(), new_call_cq.get(), &iserver, log);

    // rpc event "read done / write done / close(already connected)" call-back by this completion queue
    auto handle_calls_completion = [&]()
    {
        void * tag;
        bool ok;
        while (true)
        {
            GPR_ASSERT(new_call_cq->Next(&tag, &ok));
            if (!ok)
            {
                LOG_WARNING(log, "Client has gone away.");
                delete static_cast<CallDataQuery *>(tag);
                continue;
            }
            auto thread = ThreadFromGlobalPool{&CallDataQuery::respond, static_cast<CallDataQuery *>(tag)};
            thread.detach();
        }
    };
    // rpc event "new connection / close(waiting for connect)" call-back by this completion queue
    auto handle_requests_completion = [&] {
        void * tag;
        bool ok;
        while (true)
        {
            GPR_ASSERT(notification_cq->Next(&tag, &ok));
            if (!ok)
            {
                LOG_WARNING(log, "Client has gone away.");
                delete static_cast<CallDataQuery *>(tag);
                continue;
            }
            auto thread = ThreadFromGlobalPool{&CallDataQuery::respond, static_cast<CallDataQuery *>(tag)};
            thread.detach();
        }
    };

    auto notification_cq_thread = ThreadFromGlobalPool{handle_requests_completion};
    auto new_call_cq_thread = ThreadFromGlobalPool{handle_calls_completion};
    notification_cq_thread.detach();
    new_call_cq_thread.detach();
}

}
#endif
