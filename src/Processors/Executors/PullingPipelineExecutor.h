#pragma once

#include <Core/Block_fwd.h>
#include <atomic>
#include <memory>

namespace DB
{

class Block;
class Chunk;
class QueryPipeline;
class PullingOutputFormat;
struct ProfileInfo;

class IExecutor;
using ExecutorPtr = std::shared_ptr<IExecutor>;

/// Pulling executor for QueryPipeline. Always execute pipeline in single thread.
/// Typical usage is:
///
/// PullingPipelineExecutor executor(query_pipeline);
/// while (executor.pull(chunk))
///     ... process chunk ...
class PullingPipelineExecutor
{
public:
    /// Columns stored with automatic `LowCardinality` serialization are read as non-native `ColumnLowCardinality`
    /// columns of a `String` or `FixedString` type. The executor materializes them in everything it returns,
    /// because most consumers expect a column to match its data type. A consumer that handles the encoded
    /// representation (sends it over the native protocol, or writes it to parts) passes
    /// `keep_automatic_low_cardinality_ = true`. Other special representations (e.g. `Sparse`) are returned as is.
    explicit PullingPipelineExecutor(QueryPipeline & pipeline_, bool keep_automatic_low_cardinality_ = false);
    ~PullingPipelineExecutor();

    /// Get structure of returned block or chunk.
    const Block & getHeader() const;
    const SharedHeader & getSharedHeader() const;

    /// Methods return false if query is finished.
    /// You can use any pull method.
    bool pull(Chunk & chunk);
    bool pull(Block & block);

    /// Stop execution. It is not necessary, but helps to stop execution before executor is destroyed.
    void cancel();

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Chunk getTotals();
    Chunk getExtremes();

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Block getTotalsBlock();
    Block getExtremesBlock();

    /// Get query profile info.
    ProfileInfo & getProfileInfo();

private:
    std::atomic_bool has_data_flag = false;
    QueryPipeline & pipeline;
    std::shared_ptr<PullingOutputFormat> pulling_format;
    ExecutorPtr executor;
};

}
