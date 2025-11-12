#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <Processors/ISource.h>
#include <arrow/flight/types.h>


namespace DB
{
class ArrowFlightConnection;

class ArrowFlightSource : public ISource
{
public:
    ArrowFlightSource(std::shared_ptr<ArrowFlightConnection> connection_, const String & dataset_name_, const Block & sample_block_);
    ArrowFlightSource(std::shared_ptr<ArrowFlightConnection> connection_, std::vector<arrow::flight::FlightEndpoint> endpoints_, const Block & sample_block_);
    ArrowFlightSource(std::unique_ptr<arrow::flight::MetadataRecordBatchReader> stream_reader_, const Block & sample_block_);

    ~ArrowFlightSource() override;
    String getName() const override { return "ArrowFlightSource"; }

protected:
    Chunk generate() override;

private:
    void initializeEndpoints(const String & dataset_name_);
    bool nextEndpoint();
    void initializeSchema();

    std::shared_ptr<ArrowFlightConnection> connection;

    Block sample_block;
    std::vector<arrow::flight::FlightEndpoint> endpoints;
    size_t current_endpoint = 0;
    std::unique_ptr<arrow::flight::MetadataRecordBatchReader> stream_reader;
    std::shared_ptr<arrow::Schema> schema;
};

}

#endif
