#pragma once
#include <Common/config.h>

#if USE_CASSANDRA
#include <Storages/IStorage.h>
#include "ExternalDataSourceConfiguration.h"

#include <Processors/Transforms/CassandraSource.h>

#include "base/types.h"

namespace DB
{
class StorageCassandra final : public IStorage
{
public:
    StorageCassandra(
        const StorageID & table_id_,
        const std::string & host_,
        const UInt16 & port_,
        const std::string & keyspaces_,
        const std::string & table_name_,
        const std::string & cons_string_,
        const std::string & username_,
        const std::string & password_,
        const std::string & options_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "Cassandra"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    // SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;
    
    static StorageCassandraConfiguration getConfiguration(ASTs engine_args, ContextPtr context_);
    static CassConsistency nameConsistency(const std::string & cons_string_);
private:
    // void connectToCassandra();
    CassSessionShared getSession();
    bool allowFiltering();

    const std::string host;
    const UInt16 port;
    const std::string keyspaces;
    const std::string table_name;
    const std::string username;
    const std::string password;
    const std::string options;

    std::mutex connect_mutex;
    CassClusterPtr cluster;
    CassSessionWeak maybe_session;
    CassConsistency consistency = CASS_CONSISTENCY_ONE;
};

}

#endif

