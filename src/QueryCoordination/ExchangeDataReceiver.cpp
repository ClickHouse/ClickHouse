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
    std::unique_lock lk(mutex);
    cv.wait(lk, [this] {return !block_list.empty() || finished;});

    Block block = std::move(block_list.front());
    block_list.pop_front();

    if (!block)
    {
        LOG_DEBUG(&Poco::Logger::get("ExchangeDataReceiver"), "Fragment {} exchange id {} receive empty block from {}", fragment_id, plan_id, source);
        return {};
    }

    size_t rows = block.rows();
    LOG_DEBUG(&Poco::Logger::get("ExchangeDataReceiver"), "Fragment {} exchange id {} receive {} rows from {}", fragment_id, plan_id, rows, source);
    num_rows += rows;

    Chunk chunk(block.getColumns(), rows);

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
