#include <Processors/Sinks/DataSink.h>
#include <QueryCoordination/FragmentMgr.h>

namespace DB
{

void DataSink::consume(Chunk chunk)
{
    num_rows += chunk.getNumRows();

    auto block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (!was_begin_sent)
    {
        for (const auto & channel : channels)
        {
            if (!channel.is_local)
                channel.connection->sendExchangeData(request);
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

}
