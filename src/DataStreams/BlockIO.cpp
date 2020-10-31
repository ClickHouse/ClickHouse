#include <DataStreams/BlockIO.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryProcess.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BlockIO::BlockIO() = default;
BlockIO::BlockIO(BlockIO &&) = default;

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
    /** ProcessListEntry should be destroyed after in, after out and after pipeline,
      *  since in, out and pipeline contain pointer to objects inside ProcessListEntry (query-level MemoryTracker for example),
      *  which could be used before destroying of in and out.
      *
      *  However, QueryStatus inside ProcessListEntry holds shared pointers to streams for some reason.
      *  Streams must be destroyed before storage locks, storages and contexts inside pipeline,
      *  so releaseQueryStreams() is required.
      */
    /// TODO simplify it all

    out.reset();
    in.reset();
    if (query_process)
        query_process->queryStatus().releaseQueryStreams();
    pipeline.reset();
    query_process.reset();

    /// TODO Do we need also reset callbacks? In which order?
}

BlockIO & BlockIO::operator= (BlockIO && rhs)
{
    if (this == &rhs)
        return *this;

    /// Explicitly reset fields, so everything is destructed in right order
    reset();

    query_process           = std::move(rhs.query_process);
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

