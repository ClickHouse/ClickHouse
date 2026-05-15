#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <Processors/ISource.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <arrow/flight/types.h>


namespace DB
{
class ArrowFlightConnection;

class ArrowFlightSource : public ISource
{
    void initializeEndpoints(const String & dataset_name_);
    bool nextEndpoint();
    void initializeSchema();
    Block fillVirtualColumns(Block result_block);

public:
    ArrowFlightSource(std::shared_ptr<ArrowFlightConnection> connection_, const String & dataset_name_, const Block & sample_block_, const Block & virtual_header_, ContextPtr context_);
    ArrowFlightSource(std::shared_ptr<ArrowFlightConnection> connection_, std::vector<arrow::flight::FlightEndpoint> endpoints_, const Block & sample_block_, ContextPtr context_);
    ArrowFlightSource(std::unique_ptr<arrow::flight::MetadataRecordBatchReader> stream_reader_, const Block & sample_block_, ContextPtr context_);

protected:
    String getName() const override { return "ArrowFlightSource"; }
    Chunk generate() override;

private:
    std::shared_ptr<ArrowFlightConnection> connection;

    Block sample_block;
    Block virtual_header;
    ContextPtr context;
    std::vector<arrow::flight::FlightEndpoint> endpoints;
    size_t current_endpoint = 0;
    std::unique_ptr<arrow::flight::MetadataRecordBatchReader> stream_reader;
    std::shared_ptr<arrow::Schema> schema;
};

}

#endif
