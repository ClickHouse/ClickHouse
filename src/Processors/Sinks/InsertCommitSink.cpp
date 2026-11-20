#include <Processors/Sinks/InsertCommitSink.h>

namespace DB
{

InsertCommitSink::InsertCommitSink(SharedHeader header, std::vector<StorageWriteSessionPtr> sessions_)
    : ISink(std::move(header))
    , sessions(std::move(sessions_))
{
}

void InsertCommitSink::consume(Chunk /*chunk*/)
{
}

void InsertCommitSink::onFinish()
{
    if (isCancelled())
        return;

    for (const auto & session : sessions)
        session->commit();
}

}
