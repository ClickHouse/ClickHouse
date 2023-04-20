#include <Processors/Sources/SourceFromChunks.h>

namespace DB
{

SourceFromChunks::SourceFromChunks(Block header, Chunks && chunks_, std::optional<Chunk> && chunk_totals_, std::optional<Chunk> && chunk_extremes_)
    : ISource(header)
    , chunks(std::move(chunks_))
    , it(chunks.begin())
{
    if (chunk_totals_ != std::nullopt)
    {
        outputs.emplace_back(header, this);
        chunk_totals = std::move(chunk_totals_);
        output_totals = &outputs.back();
    }

    if (chunk_extremes_ != std::nullopt)
    {
        outputs.emplace_back(header, this);
        chunk_extremes = std::move(chunk_extremes_);
        output_extremes = &outputs.back();
    }
}

SourceFromChunks::Status SourceFromChunks::prepare()
{
    if (!finished_chunks)
    {
        Status status = ISource::prepare();

        if (status != Status::Finished)
            return status;

        finished_chunks = true;
    }

    if (getTotalsPort())
    {
        /// This logic force-pushes the data into the port instead of checking the port status first (isFinished(), canPush()). Seems to
        /// work but should be improved (TODO).
        chassert(chunk_totals.has_value());
        output_totals->push(std::move(*chunk_totals));
        output_totals->finish();
    }

    if (getExtremesPort())
    {
        chassert(chunk_extremes.has_value());
        output_extremes->push(std::move(*chunk_extremes));
        output_extremes->finish();
    }

    return Status::Finished;
}

void SourceFromChunks::work()
{
    if (!finished_chunks)
        ISource::work();
}

String SourceFromChunks::getName() const
{
    return "SourceFromChunks";
}

Chunk SourceFromChunks::generate()
{
    if (it != chunks.end())
    {
        Chunk && chunk = std::move(*it);
        it++;
        return chunk;
    }
    else
        return {};
}

}

