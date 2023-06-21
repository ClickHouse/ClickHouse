#include <QueryCoordination/DataSink.h>
#include <QueryCoordination/FragmentMgr.h>
#include <Common/logger_useful.h>

namespace DB
{

void DataSink::Channel::prepareSendData(const ExchangeDataRequest & prepare_request)
{
    if (!is_local)
        connection->sendExchangeData(prepare_request);
    else if (is_local && !local_receiver)
        local_receiver = FragmentMgr::getInstance().findReceiver(prepare_request);
}

void DataSink::Channel::sendData(Block block)
{
    if (is_local)
        local_receiver->receive(std::move(block));
    else
        connection->sendData(block, "", false);
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
            channel.prepareSendData(request);

        was_begin_sent = true;
    }

    if (output_partition.type == PartitionType::UNPARTITIONED)
    {
        for (auto & channel : channels)
            channel.sendData(std::move(block));
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
            std::vector<MutableColumns> mutable_columns(channels.size());

            for (size_t i = 0; i < channels.size(); ++i)
                mutable_columns[i] = block.cloneEmptyColumns();

            std::vector<SipHash> siphashs(rows);
            for (size_t keys_position : keys_positions)
            {
                const auto column = block.getColumns()[keys_position];
                for (size_t i = 0; i < rows; ++i)
                {
                    column->updateHashWithValue(i, siphashs[i]);
                }
            }

            for (size_t i = 0; i < rows; ++i)
            {
                size_t which_channel = siphashs[i].get64() % channels.size();

                auto & columns = mutable_columns[which_channel];
                auto src_columns = block.getColumns();
                for (size_t j = 0; j < block.columns(); ++j)
                {
                    columns[j]->insertFrom(*src_columns[j], i);
                }
            }

            for (size_t i = 0; i < channels.size(); ++i)
            {
                if (!mutable_columns[i].empty() && !mutable_columns[i][0]->empty())
                {
                    Block block_for_send = block.cloneEmpty();
                    block_for_send.setColumns(std::move(mutable_columns[i]));
                    channels[i].sendData(std::move(block_for_send));
                }
            }
        }
    }
}

void DataSink::onFinish()
{
    LOG_DEBUG(&Poco::Logger::get("DataSink"), "DataSink finish for request {}", request.toString());
    for (auto & channel : channels)
    {
        channel.sendData(Block());
    }
}

}
