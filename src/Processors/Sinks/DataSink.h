#pragma once

#include <Processors/ISink.h>
#include <Client/Connection.h>
#include <QueryCoordination/DataPartition.h>
#include <QueryCoordination/IO/ExchangeDataRequest.h>
#include <Client/ConnectionPool.h>
#include <QueryCoordination/FragmentMgr.h>

namespace DB
{

/// Sink which sends data for exchange data. like ExternalTableDataSink
class DataSink : public ISink
{
public:
    struct Channel
    {
        IConnectionPool::Entry connection;
        bool is_local;
    };

    DataSink(
        Block header,
        const std::vector<Channel> & channels_,
        DataPartition & partition,
        String query_id,
        UInt32 fragment_id,
        UInt32 exchange_id)
        : ISink(std::move(header))
        , channels(channels_)
        , output_partition(partition)
        , request(ExchangeDataRequest{.query_id = query_id, .fragment_id = fragment_id, .exchange_id = exchange_id})
    {
    }

    String getName() const override { return "DataSink"; }

    size_t getNumReadRows() const { return num_rows; }

protected:
    void consume(Chunk chunk) override
    {
        num_rows += chunk.getNumRows();

        auto block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());

        if (!was_begin_sent)
        {
            for (auto channel : channels)
            {
                if (!channel.is_local)
                    channel.connection->sendExchangeData(block, "", false);
            }
            was_begin_sent = true;
        }

        if (output_partition.type == PartitionType::UNPARTITIONED)
        {
            for (auto channel : channels)
            {
                if (channel.is_local)
                {
                    FragmentMgr::getInstance().receiveData(request, block);
                }
                else
                {
                    channel.connection->sendData(block, "", false);
                }
            }
        }
        else if (output_partition.type == PartitionType::HASH_PARTITIONED)
        {
            // TODO split by key
        }
    }

private:
    const std::vector<Channel> & channels;
    size_t num_rows = 0;
    DataPartition output_partition;

    ExchangeDataRequest request;

    bool was_begin_sent = false;
};

}
