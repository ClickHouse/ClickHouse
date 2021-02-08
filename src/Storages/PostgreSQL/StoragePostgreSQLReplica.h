#pragma once

#include "config_core.h"

#include "PostgreSQLReplicationHandler.h"
#include "PostgreSQLReplicaSettings.h"

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <ext/shared_ptr_helper.h>


namespace DB
{

class StoragePostgreSQLReplica final : public ext::shared_ptr_helper<StoragePostgreSQLReplica>, public IStorage
{
    friend struct ext::shared_ptr_helper<StoragePostgreSQLReplica>;

public:
    StoragePostgreSQLReplica(
        const StorageID & table_id_,
        const String & metadata_path_,
        const StorageInMemoryMetadata & storage_metadata,
        const Context & context_);

    StoragePostgreSQLReplica(
        const StorageID & table_id_,
        StoragePtr nested_storage_,
        const Context & context_);

    String getName() const override { return "PostgreSQLReplica"; }

    void startup() override;
    void shutdown() override;

    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    /// Called right after shutdown() in case of drop query
    void shutdownFinal();

    void createNestedIfNeeded() const;

    /// Can be nullptr
    StoragePtr tryGetNested() const;

    /// Throw if impossible to get
    StoragePtr getNested() const;

    void setNestedLoaded() const { nested_loaded.store(true); }

protected:
    StoragePostgreSQLReplica(
        const StorageID & table_id_,
        const String & remote_database_name,
        const String & remote_table_name,
        const String & connection_str,
        const StorageInMemoryMetadata & storage_metadata,
        const Context & context_,
        std::unique_ptr<PostgreSQLReplicaSettings> replication_settings_);

private:
    std::shared_ptr<ASTColumnDeclaration> getMaterializedColumnsDeclaration(
            const String name, const String type, UInt64 default_value) const;

    std::shared_ptr<ASTColumns> getColumnsListFromStorage() const;

    ASTPtr getColumnDeclaration(const DataTypePtr & data_type) const;

    ASTPtr getCreateNestedTableQuery() const;

    std::string getNestedTableName() const;

    Context makeGetNestedTableContext() const;

    void dropNested();

    std::string remote_table_name;
    std::shared_ptr<Context> global_context;

    std::unique_ptr<PostgreSQLReplicaSettings> replication_settings;
    std::unique_ptr<PostgreSQLReplicationHandler> replication_handler;

    bool is_postgresql_replica_database = false;

    mutable std::atomic<bool> nested_loaded = false;
    mutable StoragePtr nested_storage;
};

}

