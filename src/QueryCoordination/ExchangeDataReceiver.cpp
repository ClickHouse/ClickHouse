#include <QueryCoordination/ExchangeDataReceiver.h>

namespace DB
{

IProcessor::Status ExchangeDataReceiver::prepare()
{
    return ISource::prepare();
}

/// Stop reading from stream if output port is finished.
void ExchangeDataReceiver::onUpdatePorts()
{

}

void ExchangeDataReceiver::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & /*storage_limits_*/)
{

}

std::optional<Chunk> ExchangeDataReceiver::tryGenerate()
{
    // TODO lock
    // TODO block_list empty wait

    std::unique_lock lk(mutex);
    cv.wait(lk, [this] {return !block_list.empty() || finished;});

    if (finished)
        return {};

    Block block = std::move(block_list.front());
    block_list.pop_front();

    if (!block)
    {
        finished = true;
        return {};
    }

    num_rows += block.rows();
//    rows += num_rows;
    Chunk chunk(block.getColumns(), num_rows);

    if (add_aggregation_info)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = block.info.bucket_num;
        info->is_overflows = block.info.is_overflows;
        chunk.setChunkInfo(std::move(info));
    }

    return chunk;
}

void ExchangeDataReceiver::onCancel()
{

}

}
