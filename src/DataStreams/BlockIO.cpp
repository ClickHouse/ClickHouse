#include <DataStreams/BlockIO.h>
#include <Interpreters/ProcessList.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BlockInputStreamPtr BlockIO::getInputStream()
{
    if (out)
        throw Exception("Cannot get input stream from BlockIO because output stream is not empty",
                        ErrorCodes::LOGICAL_ERROR);

    if (in)
        return in;

    if (pipeline.initialized())
        return std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

    throw Exception("Cannot get input stream from BlockIO because query pipeline was not initialized",
                    ErrorCodes::LOGICAL_ERROR);
}

void BlockIO::reset()
{
    /** process_list_entry should be destroyed after in, after out and after pipeline,
      *  since in, out and pipeline contain pointer to objects inside process_list_entry (query-level MemoryTracker for example),
      *  which could be used before destroying of in and out.
      *
      *  However, QueryStatus inside process_list_entry holds shared pointers to streams for some reason.
      *  Streams must be destroyed before storage locks, storages and contexts inside pipeline,
      *  so releaseQueryStreams() is required.
      */
    /// TODO simplify it all

    out.reset();
    in.reset();
    if (process_list_entry)
        process_list_entry->get().releaseQueryStreams();
    pipeline = QueryPipeline();
    process_list_entry.reset();

    /// TODO Do we need also reset callbacks? In which order?
}

BlockIO & BlockIO::operator= (BlockIO && rhs)
{
    if (this == &rhs)
        return *this;

    /// Explicitly reset fields, so everything is destructed in right order
    reset();

    process_list_entry      = std::move(rhs.process_list_entry);
    in                      = std::move(rhs.in);
    out                     = std::move(rhs.out);
    pipeline                = std::move(rhs.pipeline);

    finish_callback         = std::move(rhs.finish_callback);
    exception_callback      = std::move(rhs.exception_callback);

    null_format             = std::move(rhs.null_format);

    return *this;
}

BlockIO::~BlockIO()
{
    reset();
}

}

