#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <Core/Names.h>
#include <Storages/IStorage.h>
#include <Storages/StorageConfiguration.h>
#include <Storages/StorageFactory.h>
#include <arrow/flight/client.h>

namespace DB
{

class StorageArrowFlight : public IStorage, protected WithContext
{
public:
    struct Configuration : public StatelessTableEngineConfiguration
    {
        String host;
        int port;
        String dataset_name;
    };

    using FlightClientPtr = std::shared_ptr<arrow::flight::FlightClient>;

    StorageArrowFlight(
        const StorageID & table_id_,
        const String & host_,
        int port_,
        const String & dataset_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);

    String getName() const override { return "ArrowFlight"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool async_write) override;

    Names getColumnNames();

private:
    StorageArrowFlight::Configuration config;

    FlightClientPtr client;

    Poco::Logger * log;
};

void registerStorageArrowFlight(StorageFactory & factory);

}

#endif
