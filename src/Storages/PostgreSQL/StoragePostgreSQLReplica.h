#pragma once

#include "config_core.h"

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

#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>
#include <Interpreters/Context.h>
#include "PostgreSQLReplicationHandler.h"
#include "PostgreSQLReplicationSettings.h"
#include "buffer_fwd.h"
#include "pqxx/pqxx"

namespace DB
{

class StoragePostgreSQLReplica final : public ext::shared_ptr_helper<StoragePostgreSQLReplica>, public IStorage
{
    friend struct ext::shared_ptr_helper<StoragePostgreSQLReplica>;

public:
    String getName() const override { return "PostgreSQLReplica"; }

    void startup() override;
    void drop() override;
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


protected:
    StoragePostgreSQLReplica(
        const StorageID & table_id_,
        const String & remote_table_name_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & storage_metadata,
        const Context & context_,
        const PostgreSQLReplicationHandler & replication_handler_,
        std::unique_ptr<PostgreSQLReplicationSettings> replication_settings_);

private:
    std::shared_ptr<ASTColumnDeclaration> getMaterializedColumnsDeclaration(
            const String name, const String type, UInt64 default_value);
    std::shared_ptr<ASTColumns> getColumnsListFromStorage();
    ASTPtr getColumnDeclaration(const DataTypePtr & data_type);
    ASTPtr getCreateHelperTableQuery();

    String remote_table_name;
    String relative_data_path;
    std::shared_ptr<Context> global_context;

    std::unique_ptr<PostgreSQLReplicationSettings> replication_settings;
    std::unique_ptr<PostgreSQLReplicationHandler> replication_handler;

    /// ReplacingMergeTree table
    StoragePtr nested_storage;
};

}

