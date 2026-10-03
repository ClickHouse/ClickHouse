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

class ArrowFlightSource final : public ISource
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
    void onCancel() noexcept override;

private:
    std::shared_ptr<ArrowFlightConnection> connection;

    Block sample_block;
    Block virtual_header;
    ContextPtr context;
    UInt64 request_timeout_sec = 0;
    std::vector<arrow::flight::FlightEndpoint> endpoints;
    size_t current_endpoint = 0;
    std::shared_ptr<arrow::flight::MetadataRecordBatchReader> stream_reader;
    std::shared_ptr<arrow::Schema> schema;

    std::mutex flight_reader_mutex;
    /// Non-null only while a DoGet stream is open. Held separately from `stream_reader` because
    /// only a DoGet stream can be cancelled, and because onCancel runs on another thread while
    /// generate is blocked reading from it.
    std::shared_ptr<arrow::flight::FlightStreamReader> flight_reader TSA_GUARDED_BY(flight_reader_mutex);
};

}

#endif
