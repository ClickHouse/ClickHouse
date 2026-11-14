#include <Processors/Sinks/SinkToStorage.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SinkToStorage::SinkToStorage(SharedHeader header) : ExceptionKeepingTransform(header, header, false) {}

void SinkToStorage::onConsume(Chunk chunk)
{
    consume(chunk);
    cur_chunk = std::move(chunk);
}

void SinkToStorage::setFinishedByCommitSink()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Sink {} cannot be finished by a commit sink", getName());
}

SinkToStorage::GenerateResult SinkToStorage::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

}
