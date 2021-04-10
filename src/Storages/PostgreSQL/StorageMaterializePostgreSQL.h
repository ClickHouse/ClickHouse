#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLReplicationHandler.h"
#include "MaterializePostgreSQLSettings.h"

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

class StorageMaterializePostgreSQL final : public ext::shared_ptr_helper<StorageMaterializePostgreSQL>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMaterializePostgreSQL>;

public:
    StorageMaterializePostgreSQL(
        const StorageID & table_id_,
        const Context & context_);

    String getName() const override { return "MaterializePostgreSQL"; }

    void startup() override;

    void shutdown() override;

    void dropInnerTableIfAny(bool no_delay, const Context & context) override;

    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void createNestedIfNeeded(PostgreSQLTableStructurePtr table_structure);

    StoragePtr getNested() const;

    StoragePtr tryGetNested() const;

    Context makeNestedTableContext() const;

    void setNestedStatus(bool loaded) { nested_loaded.store(loaded); }

    bool isNestedLoaded() { return nested_loaded.load(); }

    void setStorageMetadata();

protected:
    StorageMaterializePostgreSQL(
        const StorageID & table_id_,
        const String & remote_database_name,
        const String & remote_table_name,
        const postgres::ConnectionInfo & connection_info,
        const StorageInMemoryMetadata & storage_metadata,
        const Context & context_,
        std::unique_ptr<MaterializePostgreSQLSettings> replication_settings_);

private:
    static std::shared_ptr<ASTColumnDeclaration> getMaterializedColumnsDeclaration(
            const String name, const String type, UInt64 default_value);

    ASTPtr getColumnDeclaration(const DataTypePtr & data_type) const;

    ASTPtr getCreateNestedTableQuery(PostgreSQLTableStructurePtr table_structure);

    std::string getNestedTableName() const;

    std::string remote_table_name;
    const Context global_context;

    std::unique_ptr<MaterializePostgreSQLSettings> replication_settings;
    std::unique_ptr<PostgreSQLReplicationHandler> replication_handler;

    std::atomic<bool> nested_loaded = false;
    bool is_postgresql_replica_database = false;
    StorageID nested_table_id;
    const Context nested_context;
};

}

#endif
