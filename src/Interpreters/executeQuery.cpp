#include <Common/formatReadable.h>
#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadProfileEvents.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/LimitReadBuffer.h>
#include <IO/copyData.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/CountingBlockOutputStream.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/Lexer.h>

#if !defined(ARCADIA_BUILD)
#    include <Parsers/New/parseQuery.h>  // Y_IGNORE
#endif

#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>

#include <Storages/StorageInput.h>

#include <Access/EnabledQuota.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/QueryProcess.h>
#include <Interpreters/Context.h>

#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Formats/IOutputFormat.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INTO_OUTFILE_NOT_ALLOWED;
    extern const int QUERY_WAS_CANCELLED;
}


static void checkASTSizeLimits(const IAST & ast, const Settings & settings)
{
    if (settings.max_ast_depth)
        ast.checkDepth(settings.max_ast_depth);
    if (settings.max_ast_elements)
        ast.checkSize(settings.max_ast_elements);
}


static void setQuerySpecificSettings(ASTPtr & ast, Context & context)
{
    if (auto * ast_insert_into = dynamic_cast<ASTInsertQuery *>(ast.get()))
    {
        if (ast_insert_into->watch)
            context.setSetting("output_format_enable_streaming", 1);
    }
}

static std::tuple<ASTPtr, BlockIO> executeQueryImpl(
    const char * begin,
    const char * end,
    Context & context,
    bool internal,
    QueryProcessingStage::Enum stage,
    bool has_query_tail,
    ReadBuffer * istr)
{
#if !defined(ARCADIA_BUILD)
    assert(internal || CurrentThread::get().getQueryContext());
    assert(internal || CurrentThread::get().getQueryContext()->getCurrentQueryId() == CurrentThread::getQueryId());
#endif

    const Settings & settings = context.getSettingsRef();

    ASTPtr ast;
    const char * query_end;

    /// Don't limit the size of internal queries.
    size_t max_query_size = 0;
    if (!internal) max_query_size = settings.max_query_size;

    try
    {
#if !defined(ARCADIA_BUILD)
        if (settings.use_antlr_parser)
        {
            ast = parseQuery(begin, end, max_query_size, settings.max_parser_depth, context.getCurrentDatabase());
        }
        else
        {
            ParserQuery parser(end);

            /// TODO: parser should fail early when max_query_size limit is reached.
            ast = parseQuery(parser, begin, end, "", max_query_size, settings.max_parser_depth);
        }
#else
        ParserQuery parser(end);

        /// TODO: parser should fail early when max_query_size limit is reached.
        ast = parseQuery(parser, begin, end, "", max_query_size, settings.max_parser_depth);
#endif

        /// Interpret SETTINGS clauses as early as possible (before invoking the corresponding interpreter),
        /// to allow settings to take effect.
        if (const auto * select_query = ast->as<ASTSelectQuery>())
        {
            if (auto new_settings = select_query->settings())
                InterpreterSetQuery(new_settings, context).executeForCurrentContext();
        }
        else if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
        {
            if (!select_with_union_query->list_of_selects->children.empty())
            {
                // We might have an arbitrarily complex UNION tree, so just give
                // up if the last first-order child is not a plain SELECT.
                // It is flattened later, when we process UNION ALL/DISTINCT.
                const auto * last_select = select_with_union_query->list_of_selects->children.back()->as<ASTSelectQuery>();
                if (last_select && last_select->settings())
                {
                    InterpreterSetQuery(last_select->settings(), context).executeForCurrentContext();
                }
            }
        }
        else if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
        {
            if (query_with_output->settings_ast)
                InterpreterSetQuery(query_with_output->settings_ast, context).executeForCurrentContext();
        }

        auto * insert_query = ast->as<ASTInsertQuery>();

        if (insert_query && insert_query->settings_ast)
            InterpreterSetQuery(insert_query->settings_ast, context).executeForCurrentContext();

        if (insert_query && insert_query->data)
        {
            query_end = insert_query->data;
            insert_query->has_tail = has_query_tail;
        }
        else
        {
            query_end = end;
        }
    }
    catch (...)
    {
        /// Anyway log the query.
        String query = String(begin, begin + std::min(end - begin, static_cast<ptrdiff_t>(max_query_size)));

        auto query_for_logging = QueryProcess::prepareQueryForLogging(query, context);
        QueryProcess::logQuery(query_for_logging, context, internal);

        if (!internal)
        {
            QueryProcess::onExceptionBeforeStart(query_for_logging, context, std::chrono::system_clock::now(), ast);
        }

        throw;
    }

    setQuerySpecificSettings(ast, context);

    /// Copy query into string. It will be written to log and presented in processlist. If an INSERT query, string will not include data to insertion.
    String query(begin, query_end);
    BlockIO res;

    std::shared_ptr<QueryProcess> query_process;

    try
    {
        /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
        if (context.hasQueryParameters())
        {
            ReplaceQueryParameterVisitor visitor(context.getQueryParameters());
            visitor.visit(ast);
            query = serializeAST(*ast);
        }

        /// MUST goes before any modification (except for prepared statements,
        /// since it substitute parameters and w/o them query does not contains
        /// parameters), to keep query as-is in query_log and server log.
        String query_for_logging = QueryProcess::prepareQueryForLogging(query, context);
        QueryProcess::logQuery(query_for_logging, context, internal);

        /// Propagate WITH statement to children ASTSelect.
        if (settings.enable_global_with_statement)
        {
            ApplyWithGlobalVisitor().visit(ast);
            query = serializeAST(*ast);
        }

        /// Check the limits.
        checkASTSizeLimits(*ast, settings);

        /// Put query to process list. But don't put SHOW PROCESSLIST query itself.
        if (!internal && !ast->as<ASTShowProcesslistQuery>())
        {
            query_process = std::make_shared<QueryProcess>(query_for_logging, ast, internal, context);
        }

        /// Load external tables if they were provided
        context.initializeExternalTablesIfSet();

        auto * insert_query = ast->as<ASTInsertQuery>();
        if (insert_query && insert_query->select)
        {
            /// Prepare Input storage before executing interpreter if we already got a buffer with data.
            if (istr)
            {
                ASTPtr input_function;
                insert_query->tryFindInputFunction(input_function);
                if (input_function)
                {
                    StoragePtr storage = context.executeTableFunction(input_function);
                    auto & input_storage = dynamic_cast<StorageInput &>(*storage);
                    auto input_metadata_snapshot = input_storage.getInMemoryMetadataPtr();
                    BlockInputStreamPtr input_stream = std::make_shared<InputStreamFromASTInsertQuery>(
                        ast, istr, input_metadata_snapshot->getSampleBlock(), context, input_function);
                    input_storage.setInputStream(input_stream);
                }
            }
        }
        else
            /// reset Input callbacks if query is not INSERT SELECT
            context.resetInputCallbacks();

        auto interpreter = InterpreterFactory::get(ast, context, SelectQueryOptions(stage).setInternal(internal));

        std::shared_ptr<const EnabledQuota> quota;
        if (!interpreter->ignoreQuota())
        {
            quota = context.getQuota();
            if (quota)
            {
                if (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
                {
                    quota->used(Quota::QUERY_SELECTS, 1);
                }
                else if (ast->as<ASTInsertQuery>())
                {
                    quota->used(Quota::QUERY_INSERTS, 1);
                }
                quota->used(Quota::QUERIES, 1);
                quota->checkExceeded(Quota::ERRORS);
            }
        }

        StreamLocalLimits limits;
        if (!interpreter->ignoreLimits())
        {
            limits.mode = LimitsMode::LIMITS_CURRENT;
            limits.size_limits = SizeLimits(settings.max_result_rows, settings.max_result_bytes, settings.result_overflow_mode);
        }

        {
            /// FIXME: does not supported by the QueryProcess
            OpenTelemetrySpanHolder span("IInterpreter::execute()");
            res = interpreter->execute();
            /// FIXME: due to trickery in BlockIO::reset() we cannot set res.query_process before execute()
            res.query_process = std::move(query_process);
        }

        QueryPipeline & pipeline = res.pipeline;
        bool use_processors = pipeline.initialized();

        if (const auto * insert_interpreter = typeid_cast<const InterpreterInsertQuery *>(&*interpreter))
        {
            /// Save insertion table (not table function). TODO: support remote() table function.
            auto table_id = insert_interpreter->getDatabaseTable();
            if (!table_id.empty())
                context.setInsertionTable(std::move(table_id));
        }

        if (res.query_process)
        {
            auto & query_status = res.query_process->queryStatus();
            /// Query was killed before execution
            if (query_status.isKilled())
            {
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                    "Query '{}' is killed in pending state",
                    query_status.getInfo().client_info.current_query_id);
            }
            else if (!use_processors)
            {
                query_status.setQueryStreams(res);
            }
        }

        if (use_processors)
        {
            /// Limits on the result, the quota on the result, and also callback for progress.
            /// Limits apply only to the final result.
            pipeline.setProgressCallback(context.getProgressCallback());
            pipeline.setProcessListElement(context.getProcessListElement());
            if (stage == QueryProcessingStage::Complete && !pipeline.isCompleted())
            {
                pipeline.resize(1);
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    auto transform = std::make_shared<LimitsCheckingTransform>(header, limits);
                    transform->setQuota(quota);
                    return transform;
                });
            }
        }
        else
        {
            /// Limits on the result, the quota on the result, and also callback for progress.
            /// Limits apply only to the final result.
            if (res.in)
            {
                res.in->setProgressCallback(context.getProgressCallback());
                res.in->setProcessListElement(context.getProcessListElement());
                if (stage == QueryProcessingStage::Complete)
                {
                    if (!interpreter->ignoreQuota())
                        res.in->setQuota(quota);
                    if (!interpreter->ignoreLimits())
                        res.in->setLimits(limits);
                }
            }

            if (res.out)
            {
                if (auto * stream = dynamic_cast<CountingBlockOutputStream *>(res.out.get()))
                {
                    stream->setProcessListElement(context.getProcessListElement());
                }
            }
        }

        /// Everything related to query log.
        if (res.query_process)
        {
            res.query_process->queryStart(use_processors, interpreter.get());

            /// NOTE: you cannot use res in lambda, since it will be moved.
            auto finish_callback = [query_process = res.query_process](IBlockInputStream * stream_in, IBlockOutputStream * stream_out, QueryPipeline * query_pipeline)
            {
                query_process->queryFinish(stream_in, stream_out, query_pipeline);
            };
            auto exception_callback = [query_process = res.query_process]()
            {
                query_process->queryException();
            };

            res.finish_callback = std::move(finish_callback);
            res.exception_callback = std::move(exception_callback);
        }

        if (!internal && res.in)
        {
            WriteBufferFromOwnString msg_buf;
            res.in->dumpTree(msg_buf);
            LOG_DEBUG(&Poco::Logger::get("executeQuery"), "Query pipeline:\n{}", msg_buf.str());
        }
    }
    catch (...)
    {
        /// executeQuery() may fail, hence res.query_process may not be set, do this again.
        if (query_process)
            res.query_process = std::move(query_process);
        /// Checking that query_process is set, since something before it's creation may fail, i.e.:
        /// - ReplaceQueryParameterVisitor
        if (!internal && res.query_process)
            res.query_process->queryExceptionBeforeStart();

        throw;
    }

    return std::make_tuple(ast, std::move(res));
}


BlockIO executeQuery(
    const String & query,
    Context & context,
    bool internal,
    QueryProcessingStage::Enum stage,
    bool may_have_embedded_data)
{
    ASTPtr ast;
    BlockIO streams;
    std::tie(ast, streams) = executeQueryImpl(query.data(), query.data() + query.size(), context,
        internal, stage, !may_have_embedded_data, nullptr);

    if (const auto * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
    {
        String format_name = ast_query_with_output->format
                ? getIdentifierName(ast_query_with_output->format)
                : context.getDefaultFormat();

        if (format_name == "Null")
            streams.null_format = true;
    }

    return streams;
}

BlockIO executeQuery(
    const String & query,
    Context & context,
    bool internal,
    QueryProcessingStage::Enum stage,
    bool may_have_embedded_data,
    bool allow_processors)
{
    BlockIO res = executeQuery(query, context, internal, stage, may_have_embedded_data);

    if (!allow_processors && res.pipeline.initialized())
        res.in = res.getInputStream();

    return res;
}


void executeQuery(
    ReadBuffer & istr,
    WriteBuffer & ostr,
    bool allow_into_outfile,
    Context & context,
    std::function<void(const String &, const String &, const String &, const String &)> set_result_details)
{
    PODArray<char> parse_buf;
    const char * begin;
    const char * end;

    /// If 'istr' is empty now, fetch next data into buffer.
    if (!istr.hasPendingData())
        istr.next();

    size_t max_query_size = context.getSettingsRef().max_query_size;

    bool may_have_tail;
    if (istr.buffer().end() - istr.position() > static_cast<ssize_t>(max_query_size))
    {
        /// If remaining buffer space in 'istr' is enough to parse query up to 'max_query_size' bytes, then parse inplace.
        begin = istr.position();
        end = istr.buffer().end();
        istr.position() += end - begin;
        /// Actually we don't know will query has additional data or not.
        /// But we can't check istr.eof(), because begin and end pointers will become invalid
        may_have_tail = true;
    }
    else
    {
        /// If not - copy enough data into 'parse_buf'.
        WriteBufferFromVector<PODArray<char>> out(parse_buf);
        LimitReadBuffer limit(istr, max_query_size + 1, false);
        copyData(limit, out);
        out.finalize();

        begin = parse_buf.data();
        end = begin + parse_buf.size();
        /// Can check stream for eof, because we have copied data
        may_have_tail = !istr.eof();
    }

    ASTPtr ast;
    BlockIO streams;

    std::tie(ast, streams) = executeQueryImpl(begin, end, context, false, QueryProcessingStage::Complete, may_have_tail, &istr);

    auto & pipeline = streams.pipeline;

    try
    {
        if (streams.out)
        {
            InputStreamFromASTInsertQuery in(ast, &istr, streams.out->getHeader(), context, nullptr);
            copyData(in, *streams.out);
        }
        else if (streams.in)
        {
            const auto * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());

            WriteBuffer * out_buf = &ostr;
            std::optional<WriteBufferFromFile> out_file_buf;
            if (ast_query_with_output && ast_query_with_output->out_file)
            {
                if (!allow_into_outfile)
                    throw Exception("INTO OUTFILE is not allowed", ErrorCodes::INTO_OUTFILE_NOT_ALLOWED);

                const auto & out_file = ast_query_with_output->out_file->as<ASTLiteral &>().value.safeGet<std::string>();
                out_file_buf.emplace(out_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT);
                out_buf = &*out_file_buf;
            }

            String format_name = ast_query_with_output && (ast_query_with_output->format != nullptr)
                ? getIdentifierName(ast_query_with_output->format)
                : context.getDefaultFormat();

            auto out = context.getOutputStream(format_name, *out_buf, streams.in->getHeader());

            /// Save previous progress callback if any. TODO Do it more conveniently.
            auto previous_progress_callback = context.getProgressCallback();

            /// NOTE Progress callback takes shared ownership of 'out'.
            streams.in->setProgressCallback([out, previous_progress_callback] (const Progress & progress)
            {
                if (previous_progress_callback)
                    previous_progress_callback(progress);
                out->onProgress(progress);
            });

            if (set_result_details)
                set_result_details(context.getClientInfo().current_query_id, out->getContentType(), format_name, DateLUT::instance().getTimeZone());

            copyData(*streams.in, *out, [](){ return false; }, [&out](const Block &) { out->flush(); });
        }
        else if (pipeline.initialized())
        {
            const ASTQueryWithOutput * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());

            WriteBuffer * out_buf = &ostr;
            std::optional<WriteBufferFromFile> out_file_buf;
            if (ast_query_with_output && ast_query_with_output->out_file)
            {
                if (!allow_into_outfile)
                    throw Exception("INTO OUTFILE is not allowed", ErrorCodes::INTO_OUTFILE_NOT_ALLOWED);

                const auto & out_file = typeid_cast<const ASTLiteral &>(*ast_query_with_output->out_file).value.safeGet<std::string>();
                out_file_buf.emplace(out_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT);
                out_buf = &*out_file_buf;
            }

            String format_name = ast_query_with_output && (ast_query_with_output->format != nullptr)
                                 ? getIdentifierName(ast_query_with_output->format)
                                 : context.getDefaultFormat();

            if (!pipeline.isCompleted())
            {
                pipeline.addSimpleTransform([](const Block & header)
                {
                    return std::make_shared<MaterializingTransform>(header);
                });

                auto out = context.getOutputFormat(format_name, *out_buf, pipeline.getHeader());
                out->setAutoFlush();

                /// Save previous progress callback if any. TODO Do it more conveniently.
                auto previous_progress_callback = context.getProgressCallback();

                /// NOTE Progress callback takes shared ownership of 'out'.
                pipeline.setProgressCallback([out, previous_progress_callback] (const Progress & progress)
                {
                    if (previous_progress_callback)
                        previous_progress_callback(progress);
                    out->onProgress(progress);
                });

                if (set_result_details)
                    set_result_details(context.getClientInfo().current_query_id, out->getContentType(), format_name, DateLUT::instance().getTimeZone());

                pipeline.setOutputFormat(std::move(out));
            }
            else
            {
                pipeline.setProgressCallback(context.getProgressCallback());
            }

            {
                auto executor = pipeline.execute();
                executor->execute(pipeline.getNumThreads());
            }
        }
    }
    catch (...)
    {
        streams.onException();
        throw;
    }

    streams.onFinish();
}

}
