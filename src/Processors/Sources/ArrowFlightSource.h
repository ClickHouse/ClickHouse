#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <DataTypes/DataTypeFactory.h>
#include <Processors/ISource.h>
#include <arrow/flight/api.h>
#include <arrow/table.h>

namespace DB
{
class ArrowFlightConnection;

class ArrowFlightSource : public ISource
{
public:
    ArrowFlightSource(std::shared_ptr<ArrowFlightConnection> connection_, const std::string & dataset_name_, const Block & sample_block_);
    ArrowFlightSource(std::unique_ptr<arrow::flight::MetadataRecordBatchReader> stream_reader_, const Block & sample_block_);
    ~ArrowFlightSource() override = default;

    String getName() const override { return "ArrowFlightSource"; }

protected:
    Chunk generate() override;

private:
    void initializeStream(const String & dataset_name_);

    std::shared_ptr<ArrowFlightConnection> connection;
    Block sample_block;
    std::unique_ptr<arrow::flight::MetadataRecordBatchReader> stream_reader;
    std::shared_ptr<arrow::Schema> schema;
};

}

#endif
