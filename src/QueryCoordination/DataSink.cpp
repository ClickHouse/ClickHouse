#include <QueryCoordination/DataSink.h>
#include <QueryCoordination/FragmentMgr.h>
#include <Common/logger_useful.h>

namespace DB
{

void DataSink::Channel::sendData(Block block)
{
    if (is_local)
    {
        local_receiver->receive(std::move(block));
    }
    else
    {
        connection->sendData(block, "", false);
    }
}

void DataSink::calculateKeysPositions()
{
    const auto & sample = getPort().getHeader();
    keys_positions.resize(output_partition.keys_size);
    for (size_t i = 0; i < output_partition.keys_size; ++i)
        keys_positions[i] = sample.getPositionByName(output_partition.keys[i]);
}

void DataSink::consume(Chunk chunk)
{
    size_t rows = chunk.getNumRows();
    num_rows += rows;

    auto block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());
    if (auto chunk_info = chunk.getChunkInfo())
    {
        if (const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(chunk_info.get()))
        {
            block.info.bucket_num = agg_info->bucket_num;
            block.info.is_overflows = agg_info->is_overflows;
        }
    }

    if (!was_begin_sent)
    {
        for (auto & channel : channels)
        {
            if (!channel.is_local)
            {
                channel.connection->sendExchangeData(request);
            }
            else if (channel.is_local && !channel.local_receiver)
            {
                channel.local_receiver = FragmentMgr::getInstance().findReceiver(request);
            }
        }
        was_begin_sent = true;
    }

    if (output_partition.type == PartitionType::UNPARTITIONED)
    {
        for (auto & channel : channels)
        {
            channel.sendData(std::move(block));
        }
    }
    else if (output_partition.type == PartitionType::HASH_PARTITIONED)
    {
        if (block.info.bucket_num > -1 && output_partition.partition_by_bucket_num)
        {
            size_t which_channel = block.info.bucket_num % channels.size();
            channels[which_channel].sendData(std::move(block));
        }
        else
        {
            // normal shaffle
            std::vector<SipHash> siphashs(rows);

            std::vector<Block> blocks(channels.size());

            for (size_t i = 0; i < channels.size(); ++i)
            {
                blocks[i] = block.cloneEmpty();
            }

            for (size_t j = 0; j < block.columns(); ++j)
            {
                const auto column = block.getColumns()[keys_positions[j]];
                for (size_t i = 0; i < rows; ++i)
                {
                    column->updateHashWithValue(i, siphashs[i]);
                }
            }

            for (size_t i = 0; i < rows; ++i)
            {
                size_t which_channel = siphashs[i].get64() % channels.size();

                auto columns = blocks[which_channel].mutateColumns();
                auto src_columns = block.getColumns();
                for (size_t j = 0; j < block.columns(); ++j)
                {
                    columns[j]->insertFrom(*src_columns[j], i);
                }
            }

            for (size_t i = 0; i < channels.size(); ++i)
            {
                channels[i].sendData(std::move(blocks[i]));
            }
        }
    }
}

void DataSink::onFinish()
{
    LOG_DEBUG(&Poco::Logger::get("DataSink"), "DataSink finish");
    if (output_partition.type == PartitionType::UNPARTITIONED)
    {
        for (auto channel : channels)
        {
            if (channel.is_local)
            {
                FragmentMgr::getInstance().receiveData(request, Block());
            }
            else
            {
                channel.connection->sendData(Block(), "", false);
            }
        }
    }
    else if (output_partition.type == PartitionType::HASH_PARTITIONED)
    {
        // TODO split by key
    }
}

}
