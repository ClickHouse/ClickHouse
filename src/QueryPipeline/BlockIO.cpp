#include <QueryPipeline/BlockIO.h>
#include <Interpreters/ProcessList.h>

namespace DB
{

void BlockIO::reset()
{
    /** process_list_entries should be destroyed after in, after out and after pipeline,
      *  since in, out and pipeline contain pointer to objects inside process_list_entry (query-level MemoryTracker for example),
      *  which could be used before destroying of in and out.
      *
      *  However, QueryStatus inside process_list_entry holds shared pointers to streams for some reason.
      *  Streams must be destroyed before storage locks, storages and contexts inside pipeline,
      *  so releaseQueryStreams() is required.
      */
    /// TODO simplify it all

    releaseQuerySlot();
    pipeline.reset();
    process_list_entries.clear();

    /// TODO Do we need also reset callbacks? In which order?
}

BlockIO & BlockIO::operator= (BlockIO && rhs) noexcept
{
    if (this == &rhs)
        return *this;

    /// Explicitly reset fields, so everything is destructed in right order
    reset();

    process_list_entries    = std::move(rhs.process_list_entries);
    pipeline                = std::move(rhs.pipeline);

    finalize_query_pipeline = std::move(rhs.finalize_query_pipeline);
    finish_callbacks        = std::move(rhs.finish_callbacks);
    exception_callbacks     = std::move(rhs.exception_callbacks);

    null_format             = rhs.null_format;

    return *this;
}

BlockIO::~BlockIO()
{
    reset();
}

void BlockIO::onFinish(std::chrono::system_clock::time_point finish_time)
{
    releaseQuerySlot();
    if (finalize_query_pipeline)
    {
        const QueryPipelineFinalizedInfo query_pipeline_finalized_info = finalize_query_pipeline(std::move(pipeline));
        for (const auto & callback : finish_callbacks)
        {
            callback(query_pipeline_finalized_info, finish_time);
        }
    }
    else
    {
        pipeline.reset();
    }
}

void BlockIO::onException(bool log_as_error)
{
    releaseQuerySlot();
    setAllDataSent();

    for (const auto & callback : exception_callbacks)
        callback(log_as_error);

    pipeline.cancel();
    pipeline.reset();
}

void BlockIO::onCancelOrConnectionLoss()
{
    releaseQuerySlot();
    pipeline.cancel();
    pipeline.reset();
}

void BlockIO::setAllDataSent() const
{
    /// The following queries does not have process_list_entry:
    /// - SHOW PROCESSLIST
    for (const auto & entry : process_list_entries)
    {
        if (entry)
            entry->getQueryStatus()->setAllDataSent();
    }
}

void BlockIO::releaseQuerySlot() const
{
    /// If the query executed an external query, we need to release all query slots
    for (const auto & entry : process_list_entries)
    {
        if (entry)
            entry->getQueryStatus()->releaseQuerySlot();
    }
}

}
