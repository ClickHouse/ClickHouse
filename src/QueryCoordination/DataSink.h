#pragma once

#include <Processors/ISink.h>
#include <Client/Connection.h>
#include <QueryCoordination/DataPartition.h>
#include <QueryCoordination/IO/ExchangeDataRequest.h>
#include <Client/ConnectionPool.h>

namespace DB
{

class ExchangeDataReceiver;

/// Sink which sends data for exchange data. like ExternalTableDataSink
class DataSink : public ISink
{
public:
    struct Channel
    {
        IConnectionPool::Entry connection;
        bool is_local;
        std::shared_ptr<ExchangeDataReceiver> local_receiver;

        void prepareSendData(const ExchangeDataRequest & prepare_request);
        void sendData(const Block & block);
    };

    DataSink(
        Block header,
        std::vector<Channel> & channels_,
        DataPartition & partition,
        String local_host,
        String query_id,
        Int32 fragment_id,
        Int32 exchange_id)
        : ISink(std::move(header))
        , log(&Poco::Logger::get("DataSink"))
        , channels(channels_)
        , output_partition(partition)
        , request(ExchangeDataRequest{.from_host = local_host, .query_id = query_id, .fragment_id = fragment_id, .exchange_id = exchange_id})
    {
        if (partition.keys_size)
            calculateKeysPositions();
    }

    String getName() const override { return "DataSink"; }

    size_t getNumReadRows() const { return num_rows; }

protected:
    void consume(Chunk chunk) override;

    void onFinish() override;

    void onStart() override;

    void calculateKeysPositions();

private:
    Poco::Logger * log;

    std::vector<Channel> channels;
    size_t num_rows = 0;
    DataPartition output_partition;

    ExchangeDataRequest request;

    bool was_begin_sent = false;

    DB::ColumnNumbers keys_positions;
};

}
