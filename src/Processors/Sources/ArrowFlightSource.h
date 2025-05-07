#pragma once

#include <Processors/ISource.h>
#include <DataTypes/DataTypeFactory.h>
#include <arrow/flight/api.h>
#include <arrow/table.h>

namespace DB
{

class ArrowFlightSource : public ISource
{
public:
    ArrowFlightSource(
        std::unique_ptr<arrow::flight::FlightClient> & client_,
        const std::string & query_,
        const Block & sample_block_,
        const std::vector<std::string> & column_names_,
        UInt64 max_block_size_);

    ~ArrowFlightSource() override = default;

    String getName() const override { return "ArrowFlightSource"; }

protected:
    Chunk generate() override;

private:
    std::unique_ptr<arrow::flight::FlightClient> client;
    std::string query;
    [[maybe_unused]] UInt64 max_block_size;

    Block sample_block;
    std::unique_ptr<arrow::flight::FlightStreamReader> stream_reader;
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::string> column_names;

    void initializeStream();
    void fillChunk(MutableColumns & columns, std::shared_ptr<arrow::RecordBatch> & batch);
};

}
