#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <DataTypes/DataTypeFactory.h>
#include <Processors/ISource.h>
#include <arrow/flight/api.h>
#include <arrow/table.h>

namespace DB
{

class ArrowFlightSource : public ISource
{
public:
    using FlightClientPtr = std::shared_ptr<arrow::flight::FlightClient>;

    ArrowFlightSource(
        const FlightClientPtr & client_,
        const std::string & query_,
        const Block & sample_block_,
        const std::vector<std::string> & column_names_,
        UInt64 max_block_size_);

    ~ArrowFlightSource() override = default;

    String getName() const override { return "ArrowFlightSource"; }

protected:
    Chunk generate() override;

private:
    FlightClientPtr client;
    std::string query;

    Block sample_block;
    std::unique_ptr<arrow::flight::FlightStreamReader> stream_reader;
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::string> column_names;

    void initializeStream();
};

}

#endif
