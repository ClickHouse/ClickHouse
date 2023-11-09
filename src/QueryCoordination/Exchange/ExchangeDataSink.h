#pragma once

#include <Client/Connection.h>
#include <Client/ConnectionPool.h>
#include <Processors/ISink.h>
#include <QueryCoordination/DataPartition.h>
#include <QueryCoordination/IO/ExchangeDataRequest.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>

namespace DB
{

class ExchangeDataSource;

/// Sink which sends data for exchange data. like ExternalTableExchangeDataSink
class ExchangeDataSink : public ISink
{
public:
    struct Channel
    {
        IConnectionPool::Entry connection;
        bool is_local;
        std::shared_ptr<ExchangeDataSource> local_receiver;

        void prepareSendData(const ExchangeDataRequest & prepare_request);
        void sendData(const Block & block);
    };

    ExchangeDataSink(
        Block header,
        std::vector<Channel> & channels_,
        DataPartition & partition,
        String local_host,
        String query_id,
        UInt32 fragment_id,
        UInt32 exchange_id)
        : ISink(std::move(header))
        , log(&Poco::Logger::get("ExchangeDataSink"))
        , channels(channels_)
        , output_partition(partition)
        , request(
              ExchangeDataRequest{.from_host = local_host, .query_id = query_id, .fragment_id = fragment_id, .exchange_id = exchange_id})
    {
        if (partition.keys_size)
            calculateKeysPositions();
    }


    ExchangeDataSink(
        Block header,
        std::vector<Channel> & channels_,
        const Distribution & distribution,
        String local_host,
        String query_id,
        UInt32 fragment_id,
        UInt32 exchange_id)
        : ISink(std::move(header))
        , log(&Poco::Logger::get("ExchangeDataSink"))
        , channels(channels_)
        , output_distribution(distribution)
        , request(
              ExchangeDataRequest{.from_host = local_host, .query_id = query_id, .fragment_id = fragment_id, .exchange_id = exchange_id})
    {
        if (!distribution.keys.empty())
            calculateKeysPositions();
    }

    String getName() const override { return "ExchangeDataSink"; }
    size_t getNumReadRows() const { return num_rows; }

private:
    void consume(Chunk chunk) override;

    void onFinish() override;
    void onStart() override;

    void calculateKeysPositions();
    void calcKeysPositions();

    Poco::Logger * log;

    std::vector<Channel> channels;
    size_t num_rows = 0;

    DataPartition output_partition;
    Distribution output_distribution;

    ExchangeDataRequest request;

    bool was_begin_sent = false;

    DB::ColumnNumbers keys_positions;
};

}
