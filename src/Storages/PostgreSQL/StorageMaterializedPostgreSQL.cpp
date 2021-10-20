#include "StorageMaterializedPostgreSQL.h"

#if USE_LIBPQXX
#include <base/logger_useful.h>
#include <Common/Macros.h>
#include <Common/parseAddress.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Storages/StorageFactory.h>
#include <Storages/ReadFinalForExternalReplicaStorage.h>
#include <Storages/StoragePostgreSQL.h>
#include <Core/PostgreSQL/Connection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

static const auto NESTED_TABLE_SUFFIX = "_nested";
static const auto TMP_SUFFIX = "_tmp";


/// For the case of single storage.
StorageMaterializedPostgreSQL::StorageMaterializedPostgreSQL(
    const StorageID & table_id_,
    bool is_attach_,
    const String & remote_database_name,
    const String & remote_table_name_,
    const postgres::ConnectionInfo & connection_info,
    const StorageInMemoryMetadata & storage_metadata,
    ContextPtr context_,
    std::unique_ptr<MaterializedPostgreSQLSettings> replication_settings)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , log(&Poco::Logger::get("StorageMaterializedPostgreSQL(" + postgres::formatNameForLogs(remote_database_name, remote_table_name_) + ")"))
    , is_materialized_postgresql_database(false)
    , has_nested(false)
    , nested_context(makeNestedTableContext(context_->getGlobalContext()))
    , nested_table_id(StorageID(table_id_.database_name, getNestedTableName()))
    , remote_table_name(remote_table_name_)
    , is_attach(is_attach_)
{
    if (table_id_.uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MaterializedPostgreSQL is allowed only for Atomic database");

    setInMemoryMetadata(storage_metadata);

    String replication_identifier = remote_database_name + "_" + remote_table_name_;
    replication_settings->materialized_postgresql_tables_list = remote_table_name_;

    replication_handler = std::make_unique<PostgreSQLReplicationHandler>(
            replication_identifier,
            remote_database_name,
            table_id_.database_name,
            connection_info,
            getContext(),
            is_attach,
            *replication_settings,
            /* is_materialized_postgresql_database */false);

    if (!is_attach)
    {
        replication_handler->addStorage(remote_table_name, this);
        /// Start synchronization preliminary setup immediately and throw in case of failure.
        /// It should be guaranteed that if MaterializedPostgreSQL table was created successfully, then
        /// its nested table was also created.
        replication_handler->startSynchronization(/* throw_on_error */ true);
    }
}


/// For the case of MaterializePosgreSQL database engine.
/// It is used when nested ReplacingMergeeTree table has not yet be created by replication thread.
/// In this case this storage can't be used for read queries.
StorageMaterializedPostgreSQL::StorageMaterializedPostgreSQL(
        const StorageID & table_id_,
        ContextPtr context_,
        const String & postgres_database_name,
        const String & postgres_table_name)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , log(&Poco::Logger::get("StorageMaterializedPostgreSQL(" + postgres::formatNameForLogs(postgres_database_name, postgres_table_name) + ")"))
    , is_materialized_postgresql_database(true)
    , has_nested(false)
    , nested_context(makeNestedTableContext(context_->getGlobalContext()))
    , nested_table_id(table_id_)
{
}


/// Constructor for MaterializedPostgreSQL table engine - for the case of MaterializePosgreSQL database engine.
/// It is used when nested ReplacingMergeeTree table has already been created by replication thread.
/// This storage is ready to handle read queries.
StorageMaterializedPostgreSQL::StorageMaterializedPostgreSQL(
        StoragePtr nested_storage_,
        ContextPtr context_,
        const String & postgres_database_name,
        const String & postgres_table_name)
    : IStorage(nested_storage_->getStorageID())
    , WithContext(context_->getGlobalContext())
    , log(&Poco::Logger::get("StorageMaterializedPostgreSQL(" + postgres::formatNameForLogs(postgres_database_name, postgres_table_name) + ")"))
    , is_materialized_postgresql_database(true)
    , has_nested(true)
    , nested_context(makeNestedTableContext(context_->getGlobalContext()))
    , nested_table_id(nested_storage_->getStorageID())
{
    setInMemoryMetadata(nested_storage_->getInMemoryMetadata());
}


/// A temporary clone table might be created for current table in order to update its schema and reload
/// all data in the background while current table will still handle read requests.
StoragePtr StorageMaterializedPostgreSQL::createTemporary() const
{
    auto table_id = getStorageID();
    auto tmp_table_id = StorageID(table_id.database_name, table_id.table_name + TMP_SUFFIX);

    /// If for some reason it already exists - drop it.
    auto tmp_storage = DatabaseCatalog::instance().tryGetTable(tmp_table_id, nested_context);
    if (tmp_storage)
    {
        LOG_TRACE(&Poco::Logger::get("MaterializedPostgreSQLStorage"), "Temporary table {} already exists, dropping", tmp_table_id.getNameForLogs());
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), getContext(), tmp_table_id, /* no delay */true);
    }

    auto new_context = Context::createCopy(context);
    return StorageMaterializedPostgreSQL::create(tmp_table_id, new_context, "temporary", table_id.table_name);
}


StoragePtr StorageMaterializedPostgreSQL::getNested() const
{
    return DatabaseCatalog::instance().getTable(getNestedStorageID(), nested_context);
}


StoragePtr StorageMaterializedPostgreSQL::tryGetNested() const
{
    return DatabaseCatalog::instance().tryGetTable(getNestedStorageID(), nested_context);
}


String StorageMaterializedPostgreSQL::getNestedTableName() const
{
    auto table_id = getStorageID();

    if (is_materialized_postgresql_database)
        return table_id.table_name;

    return toString(table_id.uuid) + NESTED_TABLE_SUFFIX;
}


StorageID StorageMaterializedPostgreSQL::getNestedStorageID() const
{
    if (nested_table_id.has_value())
        return nested_table_id.value();

    auto table_id = getStorageID();
    throw Exception(ErrorCodes::LOGICAL_ERROR,
            "No storageID found for inner table. ({})", table_id.getNameForLogs());
}


void StorageMaterializedPostgreSQL::createNestedIfNeeded(PostgreSQLTableStructurePtr table_structure)
{
    if (tryGetNested())
        return;

    const auto ast_create = getCreateNestedTableQuery(std::move(table_structure));
    auto table_id = getStorageID();
    auto tmp_nested_table_id = StorageID(table_id.database_name, getNestedTableName());
    LOG_DEBUG(log, "Creating clickhouse table for postgresql table {}", table_id.getNameForLogs());

    try
    {
        InterpreterCreateQuery interpreter(ast_create, nested_context);
        interpreter.execute();

        auto nested_storage = DatabaseCatalog::instance().getTable(tmp_nested_table_id, nested_context);
        /// Save storage_id with correct uuid.
        nested_table_id = nested_storage->getStorageID();
    }
    catch (Exception & e)
    {
        e.addMessage("while creating nested table: {}", tmp_nested_table_id.getNameForLogs());
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


std::shared_ptr<Context> StorageMaterializedPostgreSQL::makeNestedTableContext(ContextPtr from_context)
{
    auto new_context = Context::createCopy(from_context);
    new_context->setInternalQuery(true);
    return new_context;
}


StoragePtr StorageMaterializedPostgreSQL::prepare()
{
    auto nested_table = getNested();
    setInMemoryMetadata(nested_table->getInMemoryMetadata());
    has_nested.store(true);
    return nested_table;
}


void StorageMaterializedPostgreSQL::startup()
{
    /// replication_handler != nullptr only in case of single table engine MaterializedPostgreSQL.
    if (replication_handler && is_attach)
    {
        replication_handler->addStorage(remote_table_name, this);
        /// In case of attach table use background startup in a separate thread. First wait until connection is reachable,
        /// then check for nested table -- it should already be created.
        replication_handler->startup();
    }
}


void StorageMaterializedPostgreSQL::shutdown()
{
    if (replication_handler)
        replication_handler->shutdown();
    auto nested = tryGetNested();
    if (nested)
        nested->shutdown();
}


void StorageMaterializedPostgreSQL::dropInnerTableIfAny(bool no_delay, ContextPtr local_context)
{
    /// If it is a table with database engine MaterializedPostgreSQL - return, because delition of
    /// internal tables is managed there.
    if (is_materialized_postgresql_database)
        return;

    replication_handler->shutdownFinal();

    auto nested_table = getNested();
    if (nested_table)
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, getNestedStorageID(), no_delay);
}


NamesAndTypesList StorageMaterializedPostgreSQL::getVirtuals() const
{
    return NamesAndTypesList{
            {"_sign", std::make_shared<DataTypeInt8>()},
            {"_version", std::make_shared<DataTypeUInt64>()}
    };
}


bool StorageMaterializedPostgreSQL::needRewriteQueryWithFinal(const Names & column_names) const
{
    return needRewriteQueryWithFinalForStorage(column_names, getNested());
}


Pipe StorageMaterializedPostgreSQL::read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams)
{
    auto materialized_table_lock = lockForShare(String(), context_->getSettingsRef().lock_acquire_timeout);
    auto nested_table = getNested();
    return readFinalFromNestedStorage(nested_table, column_names, metadata_snapshot,
            query_info, context_, processed_stage, max_block_size, num_streams);
}


std::shared_ptr<ASTColumnDeclaration> StorageMaterializedPostgreSQL::getMaterializedColumnsDeclaration(
        const String name, const String type, UInt64 default_value)
{
    auto column_declaration = std::make_shared<ASTColumnDeclaration>();

    column_declaration->name = name;
    column_declaration->type = makeASTFunction(type);

    column_declaration->default_specifier = "MATERIALIZED";
    column_declaration->default_expression = std::make_shared<ASTLiteral>(default_value);

    column_declaration->children.emplace_back(column_declaration->type);
    column_declaration->children.emplace_back(column_declaration->default_expression);

    return column_declaration;
}


ASTPtr StorageMaterializedPostgreSQL::getColumnDeclaration(const DataTypePtr & data_type) const
{
    WhichDataType which(data_type);

    if (which.isNullable())
        return makeASTFunction("Nullable", getColumnDeclaration(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));

    if (which.isArray())
        return makeASTFunction("Array", getColumnDeclaration(typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType()));

    /// getName() for decimal returns 'Decimal(precision, scale)', will get an error with it
    if (which.isDecimal())
    {
        auto make_decimal_expression = [&](std::string type_name)
        {
            auto ast_expression = std::make_shared<ASTFunction>();

            ast_expression->name = type_name;
            ast_expression->arguments = std::make_shared<ASTExpressionList>();
            ast_expression->arguments->children.emplace_back(std::make_shared<ASTLiteral>(getDecimalScale(*data_type)));

            return ast_expression;
        };

        if (which.isDecimal32())
            return make_decimal_expression("Decimal32");

        if (which.isDecimal64())
            return make_decimal_expression("Decimal64");

        if (which.isDecimal128())
            return make_decimal_expression("Decimal128");

        if (which.isDecimal256())
            return make_decimal_expression("Decimal256");
    }

    if (which.isDateTime64())
    {
        auto ast_expression = std::make_shared<ASTFunction>();

        ast_expression->name = "DateTime64";
        ast_expression->arguments = std::make_shared<ASTExpressionList>();
        ast_expression->arguments->children.emplace_back(std::make_shared<ASTLiteral>(UInt32(6)));
        return ast_expression;
    }

    return std::make_shared<ASTIdentifier>(data_type->getName());
}


/// For single storage MaterializedPostgreSQL get columns and primary key columns from storage definition.
/// For database engine MaterializedPostgreSQL get columns and primary key columns by fetching from PostgreSQL, also using the same
/// transaction with snapshot, which is used for initial tables dump.
ASTPtr StorageMaterializedPostgreSQL::getCreateNestedTableQuery(PostgreSQLTableStructurePtr table_structure)
{
    auto create_table_query = std::make_shared<ASTCreateQuery>();

    auto table_id = getStorageID();
    create_table_query->table = getNestedTableName();
    create_table_query->database = table_id.database_name;
    if (is_materialized_postgresql_database)
        create_table_query->uuid = table_id.uuid;

    auto columns_declare_list = std::make_shared<ASTColumns>();
    auto columns_expression_list = std::make_shared<ASTExpressionList>();
    auto order_by_expression = std::make_shared<ASTFunction>();

    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & columns = metadata_snapshot->getColumns();
    NamesAndTypesList ordinary_columns_and_types;

    if (!is_materialized_postgresql_database)
    {
        ordinary_columns_and_types = columns.getOrdinary();
    }
    else
    {
        if (!table_structure)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "No table structure returned for table {}.{}", table_id.database_name, table_id.table_name);
        }

        if (!table_structure->columns)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "No columns returned for table {}.{}", table_id.database_name, table_id.table_name);
        }

        ordinary_columns_and_types = *table_structure->columns;

        if (!table_structure->primary_key_columns && !table_structure->replica_identity_columns)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table {}.{} has no primary key and no replica identity index", table_id.database_name, table_id.table_name);
        }

        NamesAndTypesList merging_columns;
        if (table_structure->primary_key_columns)
            merging_columns = *table_structure->primary_key_columns;
        else
            merging_columns = *table_structure->replica_identity_columns;

        order_by_expression->name = "tuple";
        order_by_expression->arguments = std::make_shared<ASTExpressionList>();

        for (const auto & column : merging_columns)
            order_by_expression->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
    }

    for (const auto & [name, type] : ordinary_columns_and_types)
    {
        const auto & column_declaration = std::make_shared<ASTColumnDeclaration>();

        column_declaration->name = name;
        column_declaration->type = getColumnDeclaration(type);

        columns_expression_list->children.emplace_back(column_declaration);
    }

    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);

    columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_sign", "Int8", 1));
    columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_version", "UInt64", 1));

    create_table_query->set(create_table_query->columns_list, columns_declare_list);

    /// Not nullptr for single storage (because throws exception if not specified), nullptr otherwise.
    auto primary_key_ast = getInMemoryMetadataPtr()->getPrimaryKeyAST();

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, makeASTFunction("ReplacingMergeTree", std::make_shared<ASTIdentifier>("_version")));

    if (primary_key_ast)
        storage->set(storage->order_by, primary_key_ast);
    else
        storage->set(storage->order_by, order_by_expression);

    create_table_query->set(create_table_query->storage, storage);

    /// Add columns _sign and _version, so that they can be accessed from nested ReplacingMergeTree table if needed.
    ordinary_columns_and_types.push_back({"_sign", std::make_shared<DataTypeInt8>()});
    ordinary_columns_and_types.push_back({"_version", std::make_shared<DataTypeUInt64>()});

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(ordinary_columns_and_types));
    storage_metadata.setConstraints(metadata_snapshot->getConstraints());

    setInMemoryMetadata(storage_metadata);

    return create_table_query;
}


void registerStorageMaterializedPostgreSQL(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setConstraints(args.constraints);

        if (!args.storage_def->order_by && args.storage_def->primary_key)
            args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

        if (!args.storage_def->order_by)
            throw Exception("Storage MaterializedPostgreSQL needs order by key or primary key", ErrorCodes::BAD_ARGUMENTS);

        if (args.storage_def->primary_key)
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
        else
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, args.getContext());

        auto configuration = StoragePostgreSQL::getConfiguration(args.engine_args, args.getContext());
        auto connection_info = postgres::formatConnectionString(
            configuration.database, configuration.host, configuration.port,
            configuration.username, configuration.password);

        bool has_settings = args.storage_def->settings;
        auto postgresql_replication_settings = std::make_unique<MaterializedPostgreSQLSettings>();

        if (has_settings)
            postgresql_replication_settings->loadFromQuery(*args.storage_def);

        return StorageMaterializedPostgreSQL::create(
                args.table_id, args.attach, configuration.database, configuration.table, connection_info,
                metadata, args.getContext(),
                std::move(postgresql_replication_settings));
    };

    factory.registerStorage(
            "MaterializedPostgreSQL",
            creator_fn,
            StorageFactory::StorageFeatures{
                .supports_settings = true,
                .supports_sort_order = true,
                .source_access_type = AccessType::POSTGRES,
    });
}

}

#endif
