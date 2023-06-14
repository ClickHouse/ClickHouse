#pragma once

#include <Processors/ISink.h>
#include <Client/Connection.h>
#include <QueryCoordination/DataPartition.h>
#include <QueryCoordination/IO/ExchangeDataRequest.h>
#include <Client/ConnectionPool.h>

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
        std::vector<Channel> & channels_,
        DataPartition & partition,
        String query_id,
        Int32 fragment_id,
        Int32 exchange_id)
        : ISink(std::move(header))
        , channels(channels_)
        , output_partition(partition)
        , request(ExchangeDataRequest{.query_id = query_id, .fragment_id = fragment_id, .exchange_id = exchange_id})
    {
    }

    String getName() const override { return "DataSink"; }

    size_t getNumReadRows() const { return num_rows; }

protected:
    void consume(Chunk chunk) override;

private:
    std::vector<Channel> channels;
    size_t num_rows = 0;
    DataPartition output_partition;

    ExchangeDataRequest request;

    bool was_begin_sent = false;
};

}
