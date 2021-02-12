#include "StoragePostgreSQLReplica.h"

#include <Common/Macros.h>
#include <Core/Settings.h>
#include <Common/parseAddress.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Storages/StorageFactory.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

static const auto NESTED_STORAGE_SUFFIX = "_ReplacingMergeTree";


StoragePostgreSQLReplica::StoragePostgreSQLReplica(
    const StorageID & table_id_,
    const String & remote_database_name,
    const String & remote_table_name_,
    const String & connection_str,
    const StorageInMemoryMetadata & storage_metadata,
    const Context & context_,
    std::unique_ptr<PostgreSQLReplicaSettings> replication_settings_)
    : IStorage(table_id_)
    , remote_table_name(remote_table_name_)
    , global_context(std::make_shared<Context>(context_.getGlobalContext()))
    , replication_settings(std::move(replication_settings_))
    , is_postgresql_replica_database(
            DatabaseCatalog::instance().getDatabase(getStorageID().database_name)->getEngineName() == "PostgreSQLReplica")
{
    setInMemoryMetadata(storage_metadata);

    is_postgresql_replica_database = DatabaseCatalog::instance().getDatabase(
            getStorageID().database_name)->getEngineName() == "PostgreSQLReplica";

    auto metadata_path = DatabaseCatalog::instance().getDatabase(getStorageID().database_name)->getMetadataPath()
                       +  "/.metadata_" + table_id_.database_name + "_" + table_id_.table_name;

    replication_handler = std::make_unique<PostgreSQLReplicationHandler>(
            remote_database_name,
            connection_str,
            metadata_path,
            global_context,
            global_context->getMacros()->expand(replication_settings->postgresql_replication_slot_name.value),
            global_context->getMacros()->expand(replication_settings->postgresql_publication_name.value),
            replication_settings->postgresql_max_block_size.changed
                     ? replication_settings->postgresql_max_block_size.value
                     : (global_context->getSettingsRef().max_insert_block_size.value)

    );
}


StoragePostgreSQLReplica::StoragePostgreSQLReplica(
    const StorageID & table_id_,
    StoragePtr nested_storage_,
    const Context & context_)
    : IStorage(table_id_)
    , global_context(std::make_shared<Context>(context_))
    , nested_storage(nested_storage_)
    , is_postgresql_replica_database(
            DatabaseCatalog::instance().getDatabase(getStorageID().database_name)->getEngineName() == "PostgreSQLReplica")
{

}


std::string StoragePostgreSQLReplica::getNestedTableName() const
{
    auto table_name = getStorageID().table_name;

    if (!is_postgresql_replica_database)
        table_name += NESTED_STORAGE_SUFFIX;

    return table_name;
}


std::shared_ptr<ASTColumnDeclaration> StoragePostgreSQLReplica::getMaterializedColumnsDeclaration(
        const String name, const String type, UInt64 default_value) const
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


ASTPtr StoragePostgreSQLReplica::getColumnDeclaration(const DataTypePtr & data_type) const
{
    WhichDataType which(data_type);

    if (which.isNullable())
        return makeASTFunction("Nullable", getColumnDeclaration(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));

    if (which.isArray())
        return makeASTFunction("Array", getColumnDeclaration(typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType()));

    /// getName() for decimal returns 'Decimal(precison, scale)', will get an error with it
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

    return std::make_shared<ASTIdentifier>(data_type->getName());
}


/// For single storage PostgreSQLReplica get columns and primary key columns from storage definition.
/// For database engine PostgreSQLReplica get columns and primary key columns by fetching from PostgreSQL, also using the same
/// transaction with snapshot, which is used for initial tables dump.
ASTPtr StoragePostgreSQLReplica::getCreateNestedTableQuery(const std::function<PostgreSQLTableStructure()> & fetch_table_structure)
{
    auto create_table_query = std::make_shared<ASTCreateQuery>();

    auto table_id = getStorageID();
    create_table_query->table = getNestedTableName();
    create_table_query->database = table_id.database_name;

    auto columns_declare_list = std::make_shared<ASTColumns>();
    auto columns_expression_list = std::make_shared<ASTExpressionList>();
    auto order_by_expression = std::make_shared<ASTFunction>();

    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & columns = metadata_snapshot->getColumns();
    NamesAndTypesList ordinary_columns_and_types;

    if (!columns.empty())
    {
        ordinary_columns_and_types = columns.getOrdinary();
    }
    else
    {
        auto table_structure = fetch_table_structure();

        if (!table_structure.columns)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "No columns returned for table {}.{}", table_id.database_name, table_id.table_name);
        }

        StorageInMemoryMetadata storage_metadata;

        ordinary_columns_and_types = *table_structure.columns;
        storage_metadata.setColumns(ColumnsDescription(ordinary_columns_and_types));
        setInMemoryMetadata(storage_metadata);

        if (!table_structure.primary_key_columns)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "No primary key columns returned for table {}.{}", table_id.database_name, table_id.table_name);
        }

        auto primary_key_columns = *table_structure.primary_key_columns;

        order_by_expression->name = "tuple";
        order_by_expression->arguments = std::make_shared<ASTExpressionList>();

        //TODO: check for nullable
        for (const auto & column : primary_key_columns)
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

    columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_sign", "Int8", UInt64(1)));
    columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_version", "UInt64", UInt64(1)));

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

    return create_table_query;
}


void StoragePostgreSQLReplica::createNestedIfNeeded(const std::function<PostgreSQLTableStructure()> & fetch_table_structure)
{
    nested_storage = tryGetNested();

    if (nested_storage)
        return;

    auto context = makeNestedTableContext();
    const auto ast_create = getCreateNestedTableQuery(fetch_table_structure);

    InterpreterCreateQuery interpreter(ast_create, context);
    interpreter.execute();

    nested_storage = getNested();
}


Context StoragePostgreSQLReplica::makeNestedTableContext() const
{
    auto context(*global_context);
    context.makeQueryContext();
    context.addQueryFactoriesInfo(Context::QueryLogFactories::Storage, "ReplacingMergeTree");

    return context;
}


StoragePtr StoragePostgreSQLReplica::getNested()
{
    if (nested_storage)
        return nested_storage;

    auto context = makeNestedTableContext();
    nested_storage = DatabaseCatalog::instance().getTable(
            StorageID(getStorageID().database_name, getNestedTableName()), context);

    return nested_storage;
}


StoragePtr StoragePostgreSQLReplica::tryGetNested()
{
    if (nested_storage)
        return nested_storage;

    auto context = makeNestedTableContext();
    nested_storage = DatabaseCatalog::instance().tryGetTable(
            StorageID(getStorageID().database_name, getNestedTableName()), context);

    return nested_storage;
}


void StoragePostgreSQLReplica::startup()
{
    if (!is_postgresql_replica_database)
    {
        replication_handler->addStorage(remote_table_name, this);
        replication_handler->startup();
    }
}


void StoragePostgreSQLReplica::shutdown()
{
    if (replication_handler)
        replication_handler->shutdown();
}


void StoragePostgreSQLReplica::shutdownFinal()
{
    if (is_postgresql_replica_database)
        return;

    if (replication_handler)
        replication_handler->shutdownFinal();

    if (nested_storage)
        dropNested();
}


void StoragePostgreSQLReplica::dropNested()
{
    auto table_id = nested_storage->getStorageID();
    auto ast_drop = std::make_shared<ASTDropQuery>();

    ast_drop->kind = ASTDropQuery::Drop;
    ast_drop->table = table_id.table_name;
    ast_drop->database = table_id.database_name;
    ast_drop->if_exists = true;

    auto context = makeNestedTableContext();
    auto interpreter = InterpreterDropQuery(ast_drop, context);
    interpreter.execute();
}


NamesAndTypesList StoragePostgreSQLReplica::getVirtuals() const
{
    return NamesAndTypesList{};
}


Pipe StoragePostgreSQLReplica::read(
        const Names & column_names,
        const StorageMetadataPtr & /* metadata_snapshot */,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams)
{
    /// If initial table sync has not yet finished, nested tables might not be created yet.
    if (!nested_loaded)
    {
        LOG_WARNING(&Poco::Logger::get("StoragePostgreSQLReplica"), "Table {} is not loaded yet", getNestedTableName());
        return Pipe();
    }

    /// Should throw if there is no nested storage
    if (!nested_storage)
        getNested();

    auto lock = nested_storage->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);

    const StorageMetadataPtr & nested_metadata = nested_storage->getInMemoryMetadataPtr();

    Block nested_header = nested_metadata->getSampleBlock();
    ColumnWithTypeAndName & sign_column = nested_header.getByPosition(nested_header.columns() - 2);
    ColumnWithTypeAndName & version_column = nested_header.getByPosition(nested_header.columns() - 1);

    NameSet column_names_set = NameSet(column_names.begin(), column_names.end());

    if (ASTSelectQuery * select_query = query_info.query->as<ASTSelectQuery>(); select_query && !column_names_set.count(version_column.name))
    {
        auto & tables_in_select_query = select_query->tables()->as<ASTTablesInSelectQuery &>();

        if (!tables_in_select_query.children.empty())
        {
            auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();

            if (tables_element.table_expression)
                tables_element.table_expression->as<ASTTableExpression &>().final = true;
        }
    }

    String filter_column_name;
    Names require_columns_name = column_names;
    ASTPtr expressions = std::make_shared<ASTExpressionList>();
    if (column_names_set.empty() || !column_names_set.count(sign_column.name))
    {
        require_columns_name.emplace_back(sign_column.name);

        const auto & sign_column_name = std::make_shared<ASTIdentifier>(sign_column.name);
        const auto & fetch_sign_value = std::make_shared<ASTLiteral>(Field(Int8(1)));

        expressions->children.emplace_back(makeASTFunction("equals", sign_column_name, fetch_sign_value));
        filter_column_name = expressions->children.back()->getColumnName();

        for (const auto & column_name : column_names)
            expressions->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));
    }

    Pipe pipe = nested_storage->read(
            require_columns_name,
            nested_metadata, query_info, context,
            processed_stage, max_block_size, num_streams);

    pipe.addTableLock(lock);

    if (!expressions->children.empty() && !pipe.empty())
    {
        Block pipe_header = pipe.getHeader();
        auto syntax = TreeRewriter(context).analyze(expressions, pipe_header.getNamesAndTypesList());
        ExpressionActionsPtr expression_actions = ExpressionAnalyzer(expressions, syntax, context).getActions(true);

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(header, expression_actions, filter_column_name, false);
        });
    }

    return pipe;
}


void registerStoragePostgreSQLReplica(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        bool has_settings = args.storage_def->settings;
        auto postgresql_replication_settings = std::make_unique<PostgreSQLReplicaSettings>();

        if (has_settings)
            postgresql_replication_settings->loadFromQuery(*args.storage_def);

        if (engine_args.size() != 5)
            throw Exception("Storage PostgreSQLReplica requires 5 parameters: "
                            "PostgreSQL('host:port', 'database', 'table', 'username', 'password'",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setConstraints(args.constraints);

        if (!args.storage_def->order_by && args.storage_def->primary_key)
            args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

        if (!args.storage_def->order_by)
            throw Exception("Storage PostgreSQLReplica needs order by key or primary key", ErrorCodes::BAD_ARGUMENTS);

        if (args.storage_def->primary_key)
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.context);
        else
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, args.context);

        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        /// No connection is made here, see Storages/PostgreSQL/PostgreSQLConnection.cpp
        PostgreSQLConnection connection(
            remote_database,
            parsed_host_port.first,
            parsed_host_port.second,
            engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
            engine_args[4]->as<ASTLiteral &>().value.safeGet<String>());

        return StoragePostgreSQLReplica::create(
                args.table_id, remote_database, remote_table, connection.conn_str(),
                metadata, args.context,
                std::move(postgresql_replication_settings));
    };

    factory.registerStorage(
            "PostgreSQLReplica",
            creator_fn,
            StorageFactory::StorageFeatures{
                .supports_settings = true,
                .supports_sort_order = true,
                .source_access_type = AccessType::POSTGRES,
    });
}

}

