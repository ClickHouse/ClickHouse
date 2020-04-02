#pragma once

#include "ExpireUnorderedMap.h"

#include <Core/Types.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Processors/Formats/LazyOutputFormat.h>
#include <IO/RingMemoryReadWriteBuffer.h>

namespace DB
{

class WebTerminalSessionQuery
{
public:
    ~WebTerminalSessionQuery();

    void cancelQuery();

    WebTerminalSessionQuery(Context & context_, const String & execute_query, const String & query_id, bool has_vertical_output_suffix_);

    void getEchoQuery(const WriteBufferPtr & write_buffer);

    using SendHeaders = std::function<void(const ProgressValues &, size_t)>;
    bool getOutput(const WriteBufferPtr & write_buffer, const SendHeaders & set_headers);

private:
    Context & context;
    std::unique_ptr<Context> query_context;
    std::unique_ptr<CurrentThread::QueryScope> query_scope;

    std::atomic_bool finished = false;
    std::shared_ptr<RingMemoryReadWriteBuffer> out;

    BlockIO streams;
    Progress progress;
    PipelineExecutorPtr executor;
    std::atomic<size_t> total_format_rows = 0;

    ASTPtr parsed_query;
    String serialized_query;

    std::atomic_bool cancelled = false;

    template <typename Function>
    void executeQueryImpl(const Function & function);

    void executeQueryWithStreams(BlockInputStreamPtr & input, const String & output_format);

    void executeQueryWithProcessors(QueryPipeline & query_pipeline, const String & output_format);

    mutable ThreadPool background_pool{1};
};

}
