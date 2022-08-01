#include <Processors/Formats/PullingOutputFormat.h>
#include <IO/WriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

WriteBufferWithoutFinalize PullingOutputFormat::out(nullptr, 0);

void PullingOutputFormat::consume(Chunk chunk)
{
    if (data)
        throw Exception("PullingOutputFormat cannot consume chunk because it already has data",
                        ErrorCodes::LOGICAL_ERROR);

    if (chunk)
        info.update(chunk.getNumRows(), chunk.allocatedBytes());

    data = std::move(chunk);
    has_data_flag = true;
}

Chunk PullingOutputFormat::getChunk()
{
    auto chunk = std::move(data);
    has_data_flag = false;
    return chunk;
}

Chunk PullingOutputFormat::getTotals() { return std::move(totals); }
Chunk PullingOutputFormat::getExtremes() { return std::move(extremes); }

void PullingOutputFormat::setRowsBeforeLimit(size_t rows_before_limit)
{
    info.setRowsBeforeLimit(rows_before_limit);
}

}
