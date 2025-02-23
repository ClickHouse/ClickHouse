#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Common/Exception.h>
#include <DataStreams/BlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

class HudiTimeTravelQueries
{
public:
    void executeTimeTravel(const BlockInputStreamPtr & input_stream, ContextPtr context, const std::string & timestamp)
    {
        try
        {
            auto metadata = HudiMetadata::load(context);
            auto historical_data = loadHistoricalData(metadata, timestamp);
            processHistoricalData(historical_data, input_stream);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Logger::get("HudiTimeTravelQueries"), "Error executing Time Travel Query: {}", e.what());
            throw;
        }
    }

private:
    HistoricalDataPtr loadHistoricalData(const HudiMetadata & metadata, const std::string & timestamp)
    {
        HistoricalDataPtr data = std::make_shared<HistoricalData>(metadata, timestamp);
        if (!data->isValid())
        {
            throw Exception("Invalid historical data", ErrorCodes::LOGICAL_ERROR);
        }
        return data;
    }

    void processHistoricalData(const HistoricalDataPtr & data, const BlockInputStreamPtr & input_stream)
    {
        while (auto block = data->next())
        {
            input_stream->add(block);
        }
    }
};

}
