#include <Processors/ISource.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ISource::ISource(Block header, bool enable_auto_progress)
    : IProcessor({}, {std::move(header)})
    , auto_progress(enable_auto_progress)
    , output(outputs.front())
{
}

ISource::Status ISource::prepare()
{
    if (finished || isCancelled())
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (!has_input)
        return Status::Ready;

    output.pushData(std::move(current_chunk));
    has_input = false;

    if (got_exception)
    {
        finished = true;
        output.finish();
        return Status::Finished;
    }

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

void ISource::progress(size_t read_rows, size_t read_bytes)
{
    //std::cerr << "========= Progress " << read_rows << " from " << getName() << std::endl << StackTrace().toString() << std::endl;
    read_progress_was_set = true;
    read_progress.read_rows += read_rows;
    read_progress.read_bytes += read_bytes;
}

std::optional<ISource::ReadProgress> ISource::getReadProgress()
{
    ReadProgress res_progress;
    std::swap(read_progress, res_progress);
    return res_progress;
}

void ISource::work()
{
    try
    {
        read_progress_was_set = false;

        if (auto chunk = tryGenerate())
        {
            current_chunk.chunk = std::move(*chunk);
            if (current_chunk.chunk)
            {
                has_input = true;
                if (auto_progress && !read_progress_was_set)
                    progress(current_chunk.chunk.getNumRows(), current_chunk.chunk.bytes());
            }
        }
        else
            finished = true;

        if (isCancelled())
            finished = true;
    }
    catch (...)
    {
        finished = true;
        throw;
    }
}

Chunk ISource::generate()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "generate is not implemented for {}", getName());
}

std::optional<Chunk> ISource::tryGenerate()
{
    auto chunk = generate();
    if (!chunk)
        return {};

    return chunk;
}

}

