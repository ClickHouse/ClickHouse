#pragma once

#include "config.h"

#if USE_DUCKDB
#include <Storages/IStorage.h>

#include <duckdb.hpp>

namespace Poco
{
class Logger;
}

namespace DB
{

class StorageDuckDB final : public IStorage, public WithContext
{
public:
    using DuckDBPtr = std::shared_ptr<duckdb::DuckDB>;

    StorageDuckDB(
        const StorageID & table_id_,
        DuckDBPtr duckdb_,
        const String & database_path_,
        const String & remote_table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);

    std::string getName() const override { return "DuckDB"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

private:
    String remote_table_name;
    String database_path;
    DuckDBPtr duckdb_instance;
    Poco::Logger * log;
};

}

#endif
