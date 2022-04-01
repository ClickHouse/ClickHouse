#include <Processors/Transforms/MergeJoinTransform.h>
#include <base/logger_useful.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MergeJoinAlgorithm::MergeJoinAlgorithm()
    : log(&Poco::Logger::get("MergeJoinAlgorithm"))
{
}

void MergeJoinAlgorithm::initialize(Inputs inputs)
{
    LOG_DEBUG(log, "MergeJoinAlgorithm initialize, number of inputs: {}", inputs.size());
    current_inputs = std::move(inputs);
    chunks.resize(current_inputs.size());
    for (size_t i = 0; i < current_inputs.size(); ++i)
    {
        consume(current_inputs[i], i);
    }
}

static void prepareChunk(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);
}

void MergeJoinAlgorithm::consume(Input & input, size_t source_num)
{
    LOG_DEBUG(log, "Consume from {}", source_num);

    prepareChunk(input.chunk);
    chunks[source_num] = std::move(input.chunk);
}

IMergingAlgorithm::Status MergeJoinAlgorithm::merge()
{
    if (!chunks[0])
    {
        return Status(0);
    }
    if (!chunks[1])
    {
        return Status(1);
    }

    size_t rows = std::min(chunks[0].getNumRows(), chunks[1].getNumRows());
    Chunk res;

    for (auto & col : chunks[0].detachColumns())
    {
        col->cut(0, rows);
        res.addColumn(std::move(col));
    }
    for (auto & col : chunks[1].detachColumns())
    {
        col->cut(0, rows);
        res.addColumn(std::move(col));
    }
    return Status(std::move(res), true);
}

MergeJoinTransform::MergeJoinTransform(
        const Blocks & input_headers,
        const Block & output_header,
        UInt64 limit_hint)
    : IMergingTransform<MergeJoinAlgorithm>(input_headers, output_header, true, limit_hint)
    , log(&Poco::Logger::get("MergeJoinTransform"))
{
    LOG_TRACE(log, "Will use MergeJoinTransform");
}

void MergeJoinTransform::onFinish()
{
    LOG_TRACE(log, "MergeJoinTransform::onFinish");
}


}
