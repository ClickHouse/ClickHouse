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
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <ext/shared_ptr_helper.h>
#include <memory>


namespace DB
{

class StorageMaterializePostgreSQL final : public ext::shared_ptr_helper<StorageMaterializePostgreSQL>, public IStorage, WithContext
{
    friend struct ext::shared_ptr_helper<StorageMaterializePostgreSQL>;

public:
    StorageMaterializePostgreSQL(
        const StorageID & table_id_,
        ContextPtr context_);

    String getName() const override { return "MaterializePostgreSQL"; }

    void startup() override;

    void shutdown() override;

    void dropInnerTableIfAny(bool no_delay, ContextPtr local_context) override;

    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void createNestedIfNeeded(PostgreSQLTableStructurePtr table_structure);

    StoragePtr createTemporary() const;

    StoragePtr getNested() const;

    StoragePtr tryGetNested() const;

    ContextPtr getNestedTableContext() const { return nested_context; }

    void setNestedStatus(bool loaded) { nested_loaded.store(loaded); }

    bool isNestedLoaded() { return nested_loaded.load(); }

    void setStorageMetadata();

    void renameNested();

protected:
    StorageMaterializePostgreSQL(
        const StorageID & table_id_,
        const String & remote_database_name,
        const String & remote_table_name,
        const postgres::ConnectionInfo & connection_info,
        const StorageInMemoryMetadata & storage_metadata,
        ContextPtr context_,
        std::unique_ptr<MaterializePostgreSQLSettings> replication_settings_);

private:
    static std::shared_ptr<ASTColumnDeclaration> getMaterializedColumnsDeclaration(
            const String name, const String type, UInt64 default_value);

    ASTPtr getColumnDeclaration(const DataTypePtr & data_type) const;

    ASTPtr getCreateNestedTableQuery(PostgreSQLTableStructurePtr table_structure);

    std::string getNestedTableName() const;

    static std::shared_ptr<Context> makeNestedTableContext(ContextPtr from_context);

    std::string remote_table_name;
    std::unique_ptr<MaterializePostgreSQLSettings> replication_settings;
    std::unique_ptr<PostgreSQLReplicationHandler> replication_handler;
    std::atomic<bool> nested_loaded = false;
    bool is_materialize_postgresql_database = false;
    StorageID nested_table_id;
    ContextPtr nested_context;
};

}

#endif
