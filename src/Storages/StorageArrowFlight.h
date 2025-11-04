#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <Storages/IStorage.h>


namespace DB
{
class ArrowFlightConnection;
class NamedCollection;
class StorageFactory;

class StorageArrowFlight : public IStorage, protected WithContext
{
public:
    struct Configuration
    {
        String host;
        int port;
        String dataset_name;
        /// Whether the client should authenticate to the server with HTTP basic authentication.
        bool use_basic_authentication = false;
        String username;
        String password;
        bool enable_ssl = false;
        /// Path to the file containing root certificates to use for validating server certificates.
        String ssl_ca;
        /// Override the hostname checked by TLS. Use with caution.
        String ssl_override_hostname;
    };

    static Configuration getConfiguration(ASTs & engine_args, ContextPtr context);
    static Configuration processNamedCollectionResult(const NamedCollection & named_collection);

    StorageArrowFlight(
        const StorageID & table_id_,
        std::shared_ptr<ArrowFlightConnection> connection_,
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

    static ColumnsDescription getTableStructureFromData(std::shared_ptr<ArrowFlightConnection> connection_, const String & dataset_name_);

private:
    std::shared_ptr<ArrowFlightConnection> connection;
    String dataset_name;
    Poco::Logger * log;
};

void registerStorageArrowFlight(StorageFactory & factory);

}

#endif
