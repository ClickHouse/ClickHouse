#include <QueryCoordination/Exchange/ExchangeDataSource.h>

namespace DB
{

IProcessor::Status ExchangeDataSource::prepare()
{
    return ISource::prepare();
}

/// Stop reading from stream if output port is finished.
void ExchangeDataSource::onUpdatePorts()
{
}

void ExchangeDataSource::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & /*storage_limits_*/)
{
}

std::optional<Chunk> ExchangeDataSource::tryGenerate()
{
    std::unique_lock lk(mutex);
    cv.wait(lk, [this] { return !block_list.empty() || finished || isCancelled() || receive_data_exception; });

    if (unlikely(receive_data_exception))
        std::rethrow_exception(receive_data_exception);

    Block block = std::move(block_list.front());
    block_list.pop_front();

    if (!block)
    {
        LOG_DEBUG(
            &Poco::Logger::get("ExchangeDataSource"),
            "Fragment {} exchange id {} receive empty block from {}",
            fragment_id,
            plan_id,
            source);
        return {};
    }

    /// E.g select count() from distribute_table where name='a'
    /// Filter has empty header. ExchangeData has empty header. But there is real data, we construct a fake header.
    size_t rows;
    if (unlikely(!getPort().getHeader()))
    {
        rows = block.getByName("_empty_header_rows").column->get64(0);
        block.clear();
    }
    else
        rows = block.rows();

    LOG_TRACE(
        &Poco::Logger::get("ExchangeDataSource"), "Fragment {} exchange id {} receive {} rows from {} bucket_num {}", fragment_id, plan_id, rows, source, block.info.bucket_num);
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

void ExchangeDataSource::onCancel()
{
    LOG_DEBUG(&Poco::Logger::get("ExchangeDataSource"), "Fragment {} exchange id {} on cancel", fragment_id, plan_id);
    receive(Block());
}

}
